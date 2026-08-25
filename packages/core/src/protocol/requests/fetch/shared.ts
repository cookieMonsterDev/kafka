import { KafkaOffsetOutOfRange, KafkaPartialMessageError } from '../../../errors';
import { createErrorFromCode, ERROR_CODES, failure } from '../../error-codes';
import { decodeMessageSet } from '../../message-set/decoder';
import { decodeRecordBatch, type DecodedRecordBatch } from '../../records/batch';
import { Decoder } from '../../decoder';
import { Encoder } from '../../encoder';
import { ISOLATION_LEVEL } from '../../enums/isolation-level';
import {
  compactArray,
  compactNullableString,
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
  /** Verify each decoded record batch's CRC. Default `true` when omitted. */
  checkCrcs?: boolean;
  topics: FetchTopicRequest[];
  /** v7+ only (KIP-227 incremental fetch sessions); earlier request versions ignore these. */
  sessionId?: number;
  sessionEpoch?: number;
  forgottenTopics?: ForgottenTopic[];
  /** v11+ only (KIP-392 fetch from closest replica); earlier request versions ignore this. */
  rackId?: string;
  /**
   * v13+ responses (KIP-516) carry topic IDs, not names; `topicName` on each response entry is
   * resolved by matching the response's id against a topics list. An incremental fetch session
   * (KIP-227) can return data for a topic that isn't in this request's own `topics` (it's part of
   * the session but happened to be unchanged this round), so name resolution needs the caller's
   * *full* desired topic set here - not just what's on the wire. Falls back to `topics`.
   */
  topicsForResponse?: readonly FetchTopicRequest[];
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

export interface LeaderIdAndEpoch {
  leaderId: number;
  leaderEpoch: number;
}

export interface FetchNodeEndpoint {
  nodeId: number;
  host: string;
  port: number;
  rack: string | null;
}

const fetchNodeEndpointSchema = flexibleObject([
  field('nodeId', int32),
  field('host', compactString),
  field('port', int32),
  field('rack', compactNullableString),
]);
const fetchNodeEndpointsArraySchema = compactArray(fetchNodeEndpointSchema);

/**
 * Reads a Fetch partition's trailing tagged fields (v12+): DivergingEpoch (tag 0, KIP-320),
 * CurrentLeader (tag 1, KIP-951), and SnapshotId (tag 2, KIP-595). Only CurrentLeader is
 * surfaced today; DivergingEpoch and SnapshotId stay skipped until truncation detection needs
 * them. Each tag's value is `tag:uvarint, size:uvarint, <size> bytes` (KIP-482) — not the
 * compact-bytes `N+1` framing `Decoder#readTaggedFields` uses for its blanket skip.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export function readFetchPartitionTaggedFields(decoder: Decoder): LeaderIdAndEpoch | null {
  let currentLeader: LeaderIdAndEpoch | null = null;
  const numberOfTaggedFields = decoder.readUVarInt();

  for (let i = 0; i < numberOfTaggedFields; i++) {
    const tag = decoder.readUVarInt();
    const size = decoder.readUVarInt();
    const fieldDecoder = decoder.slice(size);
    decoder.forward(size);

    if (tag === 1) {
      currentLeader = { leaderId: fieldDecoder.readInt32(), leaderEpoch: fieldDecoder.readInt32() };
    }
  }

  return currentLeader;
}

/**
 * Reads the Fetch response's trailing tagged fields: NodeEndpoints (tag 0, v16+, KIP-951) — the
 * broker addresses for any `currentLeader.leaderId` values in the response the client may not
 * already have cached.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export function readFetchResponseNodeEndpoints(decoder: Decoder): FetchNodeEndpoint[] {
  let nodeEndpoints: FetchNodeEndpoint[] = [];
  const numberOfTaggedFields = decoder.readUVarInt();

  for (let i = 0; i < numberOfTaggedFields; i++) {
    const tag = decoder.readUVarInt();
    const size = decoder.readUVarInt();
    const fieldDecoder = decoder.slice(size);
    decoder.forward(size);

    if (tag === 0) {
      nodeEndpoints = fetchNodeEndpointsArraySchema.read(fieldDecoder);
    }
  }

  return nodeEndpoints;
}

/**
 * Shared by every Fetch response version: check the session-level error first (v7+, KIP-227 —
 * e.g. `FETCH_SESSION_ID_NOT_FOUND` / `INVALID_FETCH_SESSION_EPOCH`, where `responses` is empty
 * and there is nothing partition-level to report), then scan partitions for the first failure.
 * `OFFSET_OUT_OF_RANGE` becomes {@link KafkaOffsetOutOfRange} with topic and partition.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export async function parseFetchResponse<
  T extends {
    /** v7+ only; absent on earlier response versions, which have no session-level error. */
    errorCode?: number;
    responses: readonly {
      topicName: string;
      partitions: readonly { errorCode: number; partition: number; currentLeader?: LeaderIdAndEpoch | null }[];
    }[];
    nodeEndpoints?: readonly FetchNodeEndpoint[];
  },
>(data: T): Promise<T> {
  if (data.errorCode != null && failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode);
  }

  const [firstError] = data.responses.flatMap(({ topicName, partitions }) =>
    partitions
      .filter((partition) => failure(partition.errorCode))
      .map((partition) => ({ ...partition, topic: topicName })),
  );

  if (firstError) {
    const { errorCode, topic, partition, currentLeader } = firstError;
    const extras = { topic, partition, currentLeader: currentLeader ?? undefined, nodeEndpoints: data.nodeEndpoints };
    if (errorCode === OFFSET_OUT_OF_RANGE_ERROR_CODE) {
      throw new KafkaOffsetOutOfRange(createErrorFromCode(errorCode), extras);
    }
    throw createErrorFromCode(errorCode, extras);
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
export async function decodeRecordSet(decoder: Decoder, checkCrcs?: boolean): Promise<DecodedRecordBatch['records']> {
  const messagesSize = decoder.readInt32();
  if (messagesSize <= 0 || !decoder.canReadBytes(messagesSize)) {
    return [];
  }

  const messagesBuffer = decoder.readBytes(messagesSize);
  if (!messagesBuffer || messagesBuffer.length <= MAGIC_OFFSET) return [];
  return decodeRecordSetBuffer(messagesBuffer, checkCrcs);
}

/**
 * Fetch v12+ uses COMPACT_RECORDS (unsigned varint length `N+1`) instead of INT32-prefixed RECORDS.
 */
export async function decodeCompactRecordSet(
  decoder: Decoder,
  checkCrcs?: boolean,
): Promise<DecodedRecordBatch['records']> {
  const messagesBuffer = decoder.readUVarIntBytes();
  if (!messagesBuffer || messagesBuffer.length <= MAGIC_OFFSET) return [];
  return decodeRecordSetBuffer(messagesBuffer, checkCrcs);
}

async function decodeRecordSetBuffer(
  messagesBuffer: Buffer,
  checkCrcs?: boolean,
): Promise<DecodedRecordBatch['records']> {
  const messagesDecoder = new Decoder(messagesBuffer);
  const magicByte = messagesBuffer.readInt8(MAGIC_OFFSET);

  if (magicByte !== RECORD_BATCH_MAGIC) {
    return decodeMessageSet(messagesDecoder, messagesBuffer.length, checkCrcs);
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
      const batch = await decodeRecordBatch(messagesDecoder, { checkCrcs });
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
