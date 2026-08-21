import { KafkaOffsetOutOfRange, KafkaPartialMessageError } from '../../../errors';
import { createErrorFromCode, ERROR_CODES, failure } from '../../error-codes';
import { decodeMessageSet } from '../../message-set/decoder';
import { decodeRecordBatch, type DecodedRecordBatch } from '../../records/batch';
import { Decoder } from '../../decoder';
import { Encoder } from '../../encoder';
import { ISOLATION_LEVEL } from '../../enums/isolation-level';
import {
  compactArray,
  compactString,
  field,
  flexibleObject,
  int32,
  int64,
  int8,
  uuid,
  type RequestDefinition,
} from '../../schema';
import { API_KEYS } from '../api-keys';
import { ZERO_TOPIC_ID } from '../metadata/shared';

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
  /** v12+ only (KIP-320 epoch validation); earlier request versions ignore this field. */
  lastFetchedEpoch?: number;
  /** v5+ only; earlier request versions ignore this field. */
  logStartOffset?: bigint;
  maxBytes: number;
}

export interface FetchTopicRequest {
  topic: string;
  /** KIP-516 topic UUID; required on Fetch v13+ and ignored on earlier versions. */
  topicId?: Buffer;
  partitions: FetchPartitionRequest[];
}

export interface ForgottenTopic {
  topic: string;
  /** KIP-516 topic UUID; required on Fetch v13+ and ignored on earlier versions. */
  topicId?: Buffer;
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

/** First Fetch version that addresses topics by UUID instead of name (KIP-516). */
export const FETCH_TOPIC_ID_MIN_VERSION = 13;

/** First Fetch version that moves ReplicaId into tagged ReplicaState (KIP-903). */
export const FETCH_REPLICA_STATE_MIN_VERSION = 15;

/** True when `topicId` is a non-zero 16-byte UUID the broker can look up. */
export function isUsableTopicId(topicId: Buffer | undefined): topicId is Buffer {
  return topicId != null && topicId.length === 16 && !topicId.equals(ZERO_TOPIC_ID);
}

/** Topics and forgotten topics all carry a usable KIP-516 UUID. */
export function fetchRequestHasUsableTopicIds(options: FetchRequestOptions): boolean {
  return (
    options.topics.every((topic) => isUsableTopicId(topic.topicId)) &&
    (options.forgottenTopics ?? []).every((topic) => isUsableTopicId(topic.topicId))
  );
}

export function resolveFetchTopicName(topicId: Buffer, index: number, topics: readonly FetchTopicRequest[]): string {
  const byId = topics.find((entry) => entry.topicId != null && entry.topicId.equals(topicId));
  if (byId) return byId.topic;
  const byIndex = topics[index];
  return byIndex?.topic ?? '';
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
    throw createErrorFromCode(errorCode, { topic, partition });
  }

  return data;
}

/**
 * The magic byte sits at the same offset in MessageSet entries and RecordBatches: after
 * offset/firstOffset (8) + size/length (4) + crc/partitionLeaderEpoch (4).
 */
const MAGIC_OFFSET = 16;
const RECORD_BATCH_MAGIC = 2;

/**
 * Decode a Fetch `record_set`: a length-prefixed blob that may hold MessageSet entries
 * (magic 0/1) or RecordBatches (magic 2). The broker fills it up to `max_bytes` and may cut
 * the last batch short; a trailing partial batch is ignored.
 *
 * Mixed 0.10 + 0.11 formats in one response (cluster upgrading the message format) stop on
 * `KafkaUnsupportedMagicByteInMessageSet` so the next fetch can finish the RecordBatch.
 *
 * @see https://kafka.apache.org/43/implementation/messages/
 */
export async function decodeRecordSet(decoder: Decoder): Promise<DecodedRecordBatch['records']> {
  const messagesSize = decoder.readInt32();
  if (messagesSize <= 0 || !decoder.canReadBytes(messagesSize)) {
    return [];
  }

  const messagesBuffer = decoder.readBytes(messagesSize);
  if (!messagesBuffer || messagesBuffer.length <= MAGIC_OFFSET) return [];
  return decodeRecordSetBuffer(messagesBuffer);
}

/**
 * Fetch v12+ uses COMPACT_RECORDS (unsigned varint length `N+1`) instead of INT32-prefixed RECORDS.
 */
export async function decodeCompactRecordSet(decoder: Decoder): Promise<DecodedRecordBatch['records']> {
  const messagesBuffer = decoder.readUVarIntBytes();
  if (!messagesBuffer || messagesBuffer.length <= MAGIC_OFFSET) return [];
  return decodeRecordSetBuffer(messagesBuffer);
}

async function decodeRecordSetBuffer(messagesBuffer: Buffer): Promise<DecodedRecordBatch['records']> {
  const messagesDecoder = new Decoder(messagesBuffer);
  const magicByte = messagesBuffer.readInt8(MAGIC_OFFSET);

  if (magicByte !== RECORD_BATCH_MAGIC) {
    return decodeMessageSet(messagesDecoder, messagesBuffer.length);
  }

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

const partitionSchemaV13 = flexibleObject([
  field('partition', int32),
  field('currentLeaderEpoch', int32),
  field('fetchOffset', int64),
  field('lastFetchedEpoch', int32),
  field('logStartOffset', int64),
  field('maxBytes', int32),
]);
const topicSchemaV13 = flexibleObject([field('topicId', uuid), field('partitions', compactArray(partitionSchemaV13))]);
const forgottenTopicSchemaV13 = flexibleObject([field('topicId', uuid), field('partitions', compactArray(int32))]);

/** Fetch v13–v14: replicaId is still a top-level INT32; topics are UUIDs. */
export const requestSchemaV13 = flexibleObject([
  field('replicaId', int32),
  field('maxWaitTime', int32),
  field('minBytes', int32),
  field('maxBytes', int32),
  field('isolationLevel', int8),
  field('sessionId', int32),
  field('sessionEpoch', int32),
  field('topics', compactArray(topicSchemaV13)),
  field('forgottenTopics', compactArray(forgottenTopicSchemaV13)),
  field('rackId', compactString),
]);

/** Fetch v15–v18: ReplicaId is a tagged ReplicaState (omitted at consumer defaults). */
export const requestSchemaV15 = flexibleObject([
  field('maxWaitTime', int32),
  field('minBytes', int32),
  field('maxBytes', int32),
  field('isolationLevel', int8),
  field('sessionId', int32),
  field('sessionEpoch', int32),
  field('topics', compactArray(topicSchemaV13)),
  field('forgottenTopics', compactArray(forgottenTopicSchemaV13)),
  field('rackId', compactString),
]);

function encodeFetchPartitions(partitions: FetchPartitionRequest[]) {
  return partitions.map(
    ({ partition, currentLeaderEpoch, fetchOffset, lastFetchedEpoch, logStartOffset, maxBytes }) => ({
      partition,
      currentLeaderEpoch: currentLeaderEpoch ?? -1,
      fetchOffset,
      lastFetchedEpoch: lastFetchedEpoch ?? -1,
      logStartOffset: logStartOffset ?? -1n,
      maxBytes,
    }),
  );
}

/**
 * v13 replaces topic names with topic IDs (KIP-516). v14 is the same wire (KIP-405 error).
 * v15+ drops the ReplicaId INT32; ReplicaState is tagged field 1 and omitted for consumers
 * (replicaId -1 / replicaEpoch -1). v17–v18 add tagged replica directory id and high-watermark
 * on partitions, also omitted at consumer defaults.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export function createFetchRequest(apiVersion: number, options: FetchRequestOptions): RequestDefinition {
  return {
    apiKey: API_KEYS.Fetch,
    apiVersion,
    apiName: 'Fetch',
    encode: async () => {
      const topics = options.topics.map(({ topic, topicId, partitions }) => {
        if (!isUsableTopicId(topicId)) {
          throw new RangeError(`Fetch v${apiVersion} requires a 16-byte topicId for topic ${topic}`);
        }
        return { topicId, partitions: encodeFetchPartitions(partitions) };
      });
      const forgottenTopics = (options.forgottenTopics ?? []).map(({ topic, topicId, partitions }) => {
        if (!isUsableTopicId(topicId)) {
          throw new RangeError(`Fetch v${apiVersion} requires a 16-byte topicId for forgotten topic ${topic}`);
        }
        return { topicId, partitions };
      });
      const common = {
        maxWaitTime: options.maxWaitTime,
        minBytes: options.minBytes,
        maxBytes: options.maxBytes,
        isolationLevel: options.isolationLevel ?? ISOLATION_LEVEL.READ_COMMITTED,
        sessionId: options.sessionId ?? 0,
        sessionEpoch: options.sessionEpoch ?? -1,
        topics,
        forgottenTopics,
        rackId: options.rackId ?? '',
      };
      const encoder = new Encoder();
      if (apiVersion >= FETCH_REPLICA_STATE_MIN_VERSION) {
        requestSchemaV15.write(encoder, common);
      } else {
        requestSchemaV13.write(encoder, { replicaId: options.replicaId, ...common });
      }
      return encoder;
    },
  };
}
