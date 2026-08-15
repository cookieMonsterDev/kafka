import { KafkaJSPartialMessageError } from '../../errors.js'
import {
  COMPRESSION_CODEC_MASK,
  COMPRESSION_TYPES,
  lookupCodec,
  lookupCodecByAttributes,
  type CompressionType,
} from '../compression/index.js'
import { crc32c } from '../crc32c.js'
import { Decoder } from '../decoder.js'
import { Encoder } from '../encoder.js'
import { TIMESTAMP_TYPES } from '../enums/timestamp-types.js'
import { decodeRecord, type DecodedRecord, type RecordBatchContext } from './record.js'

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

const MAGIC_BYTE = 2
const TIMESTAMP_MASK = 0 // the fourth lowest bit, always 0 (since Kafka 0.10.0)
const TRANSACTIONAL_MASK = 16 // the fifth lowest bit
const TIMESTAMP_TYPE_FLAG_MASK = 0x8
const TRANSACTIONAL_FLAG_MASK = 0x10
const CONTROL_FLAG_MASK = 0x20

export interface EncodeRecordBatchOptions {
  compression?: CompressionType
  firstOffset?: bigint
  firstTimestamp?: number
  maxTimestamp?: number
  partitionLeaderEpoch?: number
  lastOffsetDelta?: number
  transactional?: boolean
  producerId?: bigint
  producerEpoch?: number
  firstSequence?: number
  /** Already-encoded records, e.g. via `encodeRecord()`. */
  records?: readonly Encoder[]
}

async function compressRecords(compression: CompressionType, records: readonly Encoder[]): Promise<Buffer> {
  const codec = lookupCodec(compression)
  if (!codec) {
    throw new Error(`Invariant violated: no codec registered for compression type ${compression}`)
  }
  return codec.compress(new Encoder().writeEncoderArray(records))
}

export async function encodeRecordBatch({
  compression = COMPRESSION_TYPES.None,
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
}: EncodeRecordBatchOptions = {}): Promise<Encoder> {
  const compressionCodecBits = compression & COMPRESSION_CODEC_MASK
  const inTransactionBit = transactional ? TRANSACTIONAL_MASK : 0
  const attributes = compressionCodecBits | TIMESTAMP_MASK | inTransactionBit

  const batchBody = new Encoder()
    .writeInt16(attributes)
    .writeInt32(lastOffsetDelta)
    .writeInt64(firstTimestamp)
    .writeInt64(maxTimestamp)
    .writeInt64(producerId)
    .writeInt16(producerEpoch)
    .writeInt32(firstSequence)

  if (compression === COMPRESSION_TYPES.None) {
    batchBody.writeArray(records, 'object')
  } else {
    const compressedRecords = await compressRecords(compression, records)
    batchBody.writeInt32(records.length).writeBuffer(compressedRecords)
  }

  // CRC32C validation happens here:
  // https://github.com/apache/kafka/blob/0.11.0.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L148
  const batch = new Encoder()
    .writeInt32(partitionLeaderEpoch)
    .writeInt8(MAGIC_BYTE)
    .writeUInt32(crc32c(batchBody.buffer))
    .writeEncoder(batchBody)

  return new Encoder().writeInt64(firstOffset).writeBytes(batch.buffer)
}

export type DecodedRecordBatch = Omit<RecordBatchContext, 'magicByte'> & {
  records: DecodedRecord[]
}

async function decodeRecords(
  codec: ReturnType<typeof lookupCodecByAttributes>,
  recordsDecoder: Decoder,
  batchContext: RecordBatchContext
): Promise<DecodedRecord[]> {
  if (!codec) {
    return recordsDecoder.readArray((decoder) => decodeOneRecord(decoder, batchContext))
  }

  const length = recordsDecoder.readInt32()
  if (length <= 0) {
    return []
  }

  const compressedRecordsBuffer = recordsDecoder.readAll()
  const decompressedRecordBuffer = await codec.decompress(compressedRecordsBuffer)
  const decompressedRecordDecoder = new Decoder(decompressedRecordBuffer)

  const records: DecodedRecord[] = new Array<DecodedRecord>(length)
  for (let i = 0; i < length; i++) {
    records[i] = decodeOneRecord(decompressedRecordDecoder, batchContext)
  }
  return records
}

function decodeOneRecord(decoder: Decoder, batchContext: RecordBatchContext): DecodedRecord {
  const recordBuffer = decoder.readVarIntBytes() ?? Buffer.alloc(0)
  return decodeRecord(new Decoder(recordBuffer), batchContext)
}

export async function decodeRecordBatch(fetchDecoder: Decoder): Promise<DecodedRecordBatch> {
  const firstOffset = fetchDecoder.readInt64()
  const length = fetchDecoder.readInt32()
  const decoder = fetchDecoder.slice(length)
  fetchDecoder.forward(length)

  const remainingBytes = Buffer.byteLength(decoder.buffer)
  if (remainingBytes < length) {
    throw new KafkaJSPartialMessageError(
      `Tried to decode a partial record batch: remainingBytes(${remainingBytes}) < recordBatchLength(${length})`
    )
  }

  const partitionLeaderEpoch = decoder.readInt32()

  // The magic byte was read by the Fetch protocol to distinguish the record batch from the
  // legacy message set. It's not used here directly, but it has to be read off the wire.
  const magicByte = decoder.readInt8()

  // The library does not currently perform CRC validation, but the field has to be read off the wire.
  decoder.readInt32()

  const attributes = decoder.readInt16()
  const lastOffsetDelta = decoder.readInt32()
  const firstTimestamp = decoder.readInt64()
  const maxTimestamp = decoder.readInt64()
  const producerId = decoder.readInt64()
  const producerEpoch = decoder.readInt16()
  const firstSequence = decoder.readInt32()

  const inTransaction = (attributes & TRANSACTIONAL_FLAG_MASK) > 0
  const isControlBatch = (attributes & CONTROL_FLAG_MASK) > 0
  const timestampType =
    (attributes & TIMESTAMP_TYPE_FLAG_MASK) > 0 ? TIMESTAMP_TYPES.LOG_APPEND_TIME : TIMESTAMP_TYPES.CREATE_TIME

  const codec = lookupCodecByAttributes(attributes)

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
  }

  const records = await decodeRecords(codec, decoder, { ...batchContext, magicByte })

  return { ...batchContext, records }
}
