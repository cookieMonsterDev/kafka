import { KafkaCorruptRecordError, KafkaPartialMessageError } from '../../errors';
import {
  COMPRESSION_CODEC_MASK,
  COMPRESSION_TYPES,
  lookupCodec,
  lookupCodecByAttributes,
  type CompressionType,
} from '../compression/index';
import { crc32c } from '../crc32c';
import { Decoder } from '../decoder';
import { Encoder } from '../encoder';
import { TIMESTAMP_TYPES } from '../enums/timestamp-types';
import { decodeRecord, type DecodedRecord, type RecordBatchContext } from './record';

/**
 * v2
 * RecordBatch =>
 *  FirstOffset => int64
 *  Length => int32
 *  PartitionLeaderEpoch => int32
 *  Magic => int8
 *  CRC => int32
 *  Attributes => int16
 *  LastOffsetDelta => int32
 *  FirstTimestamp => int64
 *  MaxTimestamp => int64
 *  ProducerId => int64
 *  ProducerEpoch => int16
 *  FirstSequence => int32
 *  Records => [Record]
 */

const MAGIC_BYTE = 2;
const TIMESTAMP_MASK = 0; // the fourth lowest bit, always 0 (since Kafka 0.10.0)
const TRANSACTIONAL_MASK = 16; // the fifth lowest bit
const TIMESTAMP_TYPE_FLAG_MASK = 0x8;
const TRANSACTIONAL_FLAG_MASK = 0x10;
const CONTROL_FLAG_MASK = 0x20;

export interface EncodeRecordBatchOptions {
  compression?: CompressionType;
  /**
   * Passed to the active codec, when it honors one. GZIP maps it straight to zlib's `level`
   * (0-9). ZSTD maps it to `zlib.constants.ZSTD_c_compressionLevel` (roughly 1-22). Snappy and
   * LZ4 have no compression-level concept in this client's codecs and ignore it.
   */
  compressionLevel?: number;
  firstOffset?: bigint;
  firstTimestamp?: number;
  maxTimestamp?: number;
  partitionLeaderEpoch?: number;
  lastOffsetDelta?: number;
  transactional?: boolean;
  producerId?: bigint;
  producerEpoch?: number;
  firstSequence?: number;
  /** Already-encoded records, e.g. via `encodeRecord()`. Ignored when `writeRecords` is set. */
  records?: readonly Encoder[];
  /** Record count on the wire. Defaults to `records.length`. Required when using `writeRecords`. */
  recordCount?: number;
  /** Write records in place into the batch encoder (or a temp encoder when compressing). */
  writeRecords?: (encoder: Encoder) => void;
  /** Encoder capacity hint in bytes (power-of-two rounded). */
  estimatedBytes?: number;
}

/** firstOffset … records-count, matching Java `DefaultRecordBatch.RECORD_BATCH_OVERHEAD`. */
const RECORD_BATCH_OVERHEAD = 61;

function sizeOfEncodedRecords(records: readonly Encoder[]): number {
  let size = 0;
  for (const record of records) {
    size += record.size();
  }
  return size;
}

async function compressEncoder(compression: CompressionType, encoder: Encoder, level?: number): Promise<Buffer> {
  const codec = lookupCodec(compression);
  if (!codec) {
    throw new Error(`Invariant violated: no codec registered for compression type ${compression}`);
  }
  return codec.compress(encoder, level);
}

async function compressRecords(
  compression: CompressionType,
  records: readonly Encoder[],
  level?: number,
): Promise<Buffer> {
  return compressEncoder(compression, new Encoder(sizeOfEncodedRecords(records)).writeEncoderArray(records), level);
}

export async function encodeRecordBatch({
  compression = COMPRESSION_TYPES.None,
  compressionLevel,
  firstOffset = 0n,
  firstTimestamp = Date.now(),
  maxTimestamp = Date.now(),
  partitionLeaderEpoch = 0,
  lastOffsetDelta = 0,
  transactional = false,
  producerId = -1n, // for idempotent messages
  producerEpoch = 0, // for idempotent messages
  firstSequence = 0, // for idempotent messages
  records = [],
  recordCount,
  writeRecords,
  estimatedBytes,
}: EncodeRecordBatchOptions = {}): Promise<Encoder> {
  const compressionCodecBits = compression & COMPRESSION_CODEC_MASK;
  const inTransactionBit = transactional ? TRANSACTIONAL_MASK : 0;
  const attributes = compressionCodecBits | TIMESTAMP_MASK | inTransactionBit;
  const count = recordCount ?? records.length;

  let compressedRecords: Buffer | undefined;
  if (compression !== COMPRESSION_TYPES.None) {
    if (writeRecords) {
      const recordsEncoder = new Encoder(estimatedBytes ?? 511);
      writeRecords(recordsEncoder);
      compressedRecords = await compressEncoder(compression, recordsEncoder, compressionLevel);
    } else {
      compressedRecords = await compressRecords(compression, records, compressionLevel);
    }
  }

  const estimatedSize =
    estimatedBytes ??
    (compressedRecords !== undefined
      ? RECORD_BATCH_OVERHEAD + compressedRecords.length
      : RECORD_BATCH_OVERHEAD + sizeOfEncodedRecords(records));

  const encoder = new Encoder(estimatedSize);
  encoder.writeInt64(firstOffset);

  const lengthOffset = encoder.size();
  encoder.writeInt32(0);

  encoder.writeInt32(partitionLeaderEpoch);
  encoder.writeInt8(MAGIC_BYTE);

  const crcOffset = encoder.size();
  encoder.writeUInt32(0);

  // CRC32C covers attributes through the last record:
  // https://github.com/apache/kafka/blob/0.11.0.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L148
  const bodyStart = encoder.size();
  encoder
    .writeInt16(attributes)
    .writeInt32(lastOffsetDelta)
    .writeInt64(firstTimestamp)
    .writeInt64(maxTimestamp)
    .writeInt64(producerId)
    .writeInt16(producerEpoch)
    .writeInt32(firstSequence);

  if (compressedRecords !== undefined) {
    encoder.writeInt32(count).writeBuffer(compressedRecords);
  } else {
    encoder.writeInt32(count);
    if (writeRecords) {
      writeRecords(encoder);
    } else {
      encoder.writeEncoderArray(records);
    }
  }

  const end = encoder.size();
  encoder.writeInt32At(lengthOffset, end - lengthOffset - 4);
  encoder.writeUInt32At(crcOffset, crc32c(encoder.buffer.subarray(bodyStart)));
  return encoder;
}

export type DecodedRecordBatch = Omit<RecordBatchContext, 'magicByte'> & {
  records: DecodedRecord[];
};

async function decodeRecords(
  codec: ReturnType<typeof lookupCodecByAttributes>,
  recordsDecoder: Decoder,
  batchContext: RecordBatchContext,
): Promise<DecodedRecord[]> {
  if (!codec) {
    return recordsDecoder.readArray((decoder) => decodeOneRecord(decoder, batchContext));
  }

  const length = recordsDecoder.readInt32();
  if (length <= 0) {
    return [];
  }

  const compressedRecordsBuffer = recordsDecoder.readAll();
  const decompressedRecordBuffer = await codec.decompress(compressedRecordsBuffer);
  const decompressedRecordDecoder = new Decoder(decompressedRecordBuffer);

  const records: DecodedRecord[] = new Array<DecodedRecord>(length);
  for (let i = 0; i < length; i++) {
    records[i] = decodeOneRecord(decompressedRecordDecoder, batchContext);
  }
  return records;
}

function decodeOneRecord(decoder: Decoder, batchContext: RecordBatchContext): DecodedRecord {
  const recordBuffer = decoder.readVarIntBytes() ?? Buffer.alloc(0);
  return decodeRecord(new Decoder(recordBuffer), batchContext);
}

export interface DecodeRecordBatchOptions {
  /**
   * Verify the batch's CRC-32C on decode and throw {@link KafkaCorruptRecordError} on mismatch.
   * Default `true` (matches `ConsumerConfig.checkCrcs`). Set `false` to skip the check for
   * throughput - silent data corruption then goes undetected.
   */
  checkCrcs?: boolean;
}

export async function decodeRecordBatch(
  fetchDecoder: Decoder,
  { checkCrcs = true }: DecodeRecordBatchOptions = {},
): Promise<DecodedRecordBatch> {
  const firstOffset = fetchDecoder.readInt64();
  const length = fetchDecoder.readInt32();
  const decoder = fetchDecoder.slice(length);
  fetchDecoder.forward(length);

  const remainingBytes = Buffer.byteLength(decoder.buffer);
  if (remainingBytes < length) {
    throw new KafkaPartialMessageError(
      `Tried to decode a partial record batch: remainingBytes(${remainingBytes}) < recordBatchLength(${length})`,
    );
  }

  const partitionLeaderEpoch = decoder.readInt32();

  // The magic byte was read by the Fetch protocol to distinguish the record batch from the
  // legacy message set. It's not used here directly, but it has to be read off the wire.
  const magicByte = decoder.readInt8();

  const crc = decoder.readUInt32();
  // CRC32C covers attributes through the last record - everything from here to the end of
  // `decoder.buffer` (which is exactly this batch's `length` bytes), matching the encode side.
  const bodyStart = decoder.offset;

  const attributes = decoder.readInt16();
  const lastOffsetDelta = decoder.readInt32();
  const firstTimestamp = decoder.readInt64();
  const maxTimestamp = decoder.readInt64();
  const producerId = decoder.readInt64();
  const producerEpoch = decoder.readInt16();
  const firstSequence = decoder.readInt32();

  const inTransaction = (attributes & TRANSACTIONAL_FLAG_MASK) > 0;
  const isControlBatch = (attributes & CONTROL_FLAG_MASK) > 0;
  const timestampType =
    (attributes & TIMESTAMP_TYPE_FLAG_MASK) > 0 ? TIMESTAMP_TYPES.LOG_APPEND_TIME : TIMESTAMP_TYPES.CREATE_TIME;

  const codec = lookupCodecByAttributes(attributes);

  const batchContext: Omit<RecordBatchContext, 'magicByte'> = {
    firstOffset,
    firstTimestamp,
    partitionLeaderEpoch,
    inTransaction,
    isControlBatch,
    lastOffsetDelta,
    producerId,
    producerEpoch,
    firstSequence,
    maxTimestamp,
    timestampType,
  };

  if (checkCrcs) {
    const computedCrc = crc32c(decoder.buffer.subarray(bodyStart));
    if (computedCrc !== crc) {
      throw new KafkaCorruptRecordError(`Record batch CRC mismatch: expected ${crc}, computed ${computedCrc}`, {
        expectedCrc: crc,
        computedCrc,
      });
    }
  }

  const records = await decodeRecords(codec, decoder, { ...batchContext, magicByte });

  return { ...batchContext, records };
}
