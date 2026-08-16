import { KafkaOffsetOutOfRange, KafkaPartialMessageError } from '../../../errors';
import { createErrorFromCode, ERROR_CODES, failure } from '../../error-codes';
import { decodeRecordBatch, type DecodedRecordBatch } from '../../records/batch';
import { Decoder } from '../../decoder';

/** The wire's `topic` field is a non-nullable STRING; every version's response decode uses this. */
export function readTopicName(decoder: Decoder): string {
  const topicName = decoder.readString();
  if (topicName === null) throw new RangeError('Expected a non-null topic name, got null');
  return topicName;
}

export interface FetchPartitionRequest {
  partition: number;
  /** v9+ only; earlier request versions ignore this field. */
  currentLeaderEpoch?: number;
  fetchOffset: bigint;
  /** v5+ only; earlier request versions ignore this field. */
  logStartOffset?: bigint;
  maxBytes: number;
}

export interface FetchTopicRequest {
  topic: string;
  partitions: FetchPartitionRequest[];
}

export interface ForgottenTopic {
  topic: string;
  partitions: number[];
}

export interface FetchRequestOptions {
  replicaId: number;
  maxWaitTime: number;
  minBytes: number;
  maxBytes: number;
  isolationLevel?: number;
  topics: FetchTopicRequest[];
  /** v7+ only (KIP-227 incremental fetch sessions); earlier request versions ignore these. */
  sessionId?: number;
  sessionEpoch?: number;
  forgottenTopics?: ForgottenTopic[];
  /** v11+ only (KIP-392 fetch from closest replica); earlier request versions ignore this. */
  rackId?: string;
}

const OFFSET_OUT_OF_RANGE_ERROR_CODE = ERROR_CODES.find((e) => e.type === 'OFFSET_OUT_OF_RANGE')?.code;

/**
 * Shared by every Fetch response version: scan partitions for the first failure.
 * `OFFSET_OUT_OF_RANGE` becomes {@link KafkaOffsetOutOfRange} with topic and partition.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export async function parseFetchResponse<
  T extends {
    responses: readonly { topicName: string; partitions: readonly { errorCode: number; partition: number }[] }[];
  },
>(data: T): Promise<T> {
  const [firstError] = data.responses.flatMap(({ topicName, partitions }) =>
    partitions
      .filter((partition) => failure(partition.errorCode))
      .map((partition) => ({ ...partition, topic: topicName })),
  );

  if (firstError) {
    const { errorCode, topic, partition } = firstError;
    if (errorCode === OFFSET_OUT_OF_RANGE_ERROR_CODE) {
      throw new KafkaOffsetOutOfRange(createErrorFromCode(errorCode), { topic, partition });
    }
    throw createErrorFromCode(errorCode);
  }

  return data;
}

/**
 * Decode a Fetch `record_set`: a length-prefixed blob that may hold several RecordBatches.
 * The broker fills it up to `max_bytes` and may cut the last batch short; a trailing partial
 * batch is ignored and the rest are flattened into one record array.
 *
 * @see https://kafka.apache.org/43/implementation/messages/
 */
export async function decodeRecordSet(decoder: Decoder): Promise<DecodedRecordBatch['records']> {
  const messagesSize = decoder.readInt32();
  if (messagesSize <= 0 || !decoder.canReadBytes(messagesSize)) {
    return [];
  }

  const messagesBuffer = decoder.readBytes(messagesSize);
  if (!messagesBuffer) return [];
  const messagesDecoder = new Decoder(messagesBuffer);

  const records: DecodedRecordBatch['records'] = [];
  // The fixed-size portion of a v2 RecordBatch header (everything up to, but not including, the
  // records themselves) is 57 bytes - firstOffset(8) + length(4) + partitionLeaderEpoch(4) +
  // magic(1) + crc(4) + attributes(2) + lastOffsetDelta(4) + firstTimestamp(8) + maxTimestamp(8) +
  // producerId(8) + producerEpoch(2) + firstSequence(4). Stop once fewer than that remain, rather
  // than attempting one more decode: reading the header fields themselves would throw a raw
  // out-of-bounds `RangeError` (from `Buffer#readInt32BE`/`readBigInt64BE`) before
  // `decodeRecordBatch`'s own `KafkaPartialMessageError` truncation check ever runs.
  const RECORD_BATCH_HEADER_SIZE = 57;
  while (messagesDecoder.canReadBytes(RECORD_BATCH_HEADER_SIZE)) {
    try {
      const batch = await decodeRecordBatch(messagesDecoder);
      records.push(...batch.records);
    } catch (e) {
      // The tail of the record batches can have incomplete records due to how max_bytes works.
      // @see https://kafka.apache.org/protocol#The_Messages_Fetch
      if (e instanceof KafkaPartialMessageError) break;
      throw e;
    }
  }

  return records;
}
