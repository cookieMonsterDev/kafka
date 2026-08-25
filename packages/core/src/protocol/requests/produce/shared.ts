import { encodeRecord, type RecordHeaders } from '../../records/record';
import { encodeRecordBatch } from '../../records/batch';
import { COMPRESSION_TYPES, type CompressionType } from '../../compression/index';
import { Decoder } from '../../decoder';
import { Encoder } from '../../encoder';
import { createErrorFromCode, failure } from '../../error-codes';
import {
  array,
  bytes,
  compactArray,
  compactBytes,
  compactNullableString,
  compactString,
  defineResponse,
  field,
  flexibleObject,
  int16,
  int32,
  int64,
  nullableString,
  object,
  string,
  uuid,
  type RequestDefinition,
} from '../../schema';
import { API_KEYS } from '../api-keys';

/** First Produce version that addresses topics by UUID instead of name (KIP-516). */
export const PRODUCE_TOPIC_ID_MIN_VERSION = 13;

const ZERO_TOPIC_ID = Buffer.alloc(16);

/** True when `topicId` is a non-zero 16-byte UUID the broker can look up. */
export function isUsableTopicId(topicId: Buffer | undefined): topicId is Buffer {
  return topicId != null && topicId.length === 16 && !topicId.equals(ZERO_TOPIC_ID);
}

export interface ProduceMessage {
  key?: Buffer | string | null;
  value?: Buffer | string | null;
  timestamp?: number;
  headers?: RecordHeaders;
}

export interface ProducePartitionData {
  partition: number;
  /** The idempotent producer's next sequence number for this partition; 0 for non-idempotent sends. */
  firstSequence?: number;
  messages: ProduceMessage[];
}

export interface ProduceTopicData {
  topic: string;
  /** KIP-516 topic UUID; required on Produce v13+ and ignored on earlier versions. */
  topicId?: Buffer;
  partitions: ProducePartitionData[];
}

export interface ProduceRequestOptions {
  acks: number;
  timeout: number;
  transactionalId?: string | null;
  producerId?: bigint;
  producerEpoch?: number;
  compression?: CompressionType;
  /** Passed to the active codec, when it honors one. @see EncodeRecordBatchOptions.compressionLevel */
  compressionLevel?: number;
  topicData: ProduceTopicData[];
}

const requestBodySchema = object([
  field('transactionalId', nullableString),
  field('acks', int16),
  field('timeout', int32),
  field(
    'topicData',
    array(
      object([
        field('topic', string),
        field('partitions', array(object([field('partition', int32), field('recordSet', bytes)]))),
      ]),
    ),
  ),
]);

async function encodePartition(
  { partition, firstSequence = 0, messages }: ProducePartitionData,
  {
    compression,
    compressionLevel,
    transactionalId,
    producerId,
    producerEpoch,
  }: {
    compression: CompressionType;
    compressionLevel?: number;
    transactionalId?: string | null;
    producerId?: bigint;
    producerEpoch?: number;
  },
): Promise<{ partition: number; recordSet: Buffer }> {
  const dateNow = Date.now();
  let firstTimestamp = dateNow;
  let maxTimestamp = dateNow;
  let seenTimestamp = false;
  let estimatedBytes = 61;

  for (const message of messages) {
    const timestamp = message.timestamp;
    if (timestamp != null) {
      if (!seenTimestamp) {
        firstTimestamp = timestamp;
        maxTimestamp = timestamp;
        seenTimestamp = true;
      } else {
        if (timestamp < firstTimestamp) firstTimestamp = timestamp;
        if (timestamp > maxTimestamp) maxTimestamp = timestamp;
      }
    }

    estimatedBytes += 32;
    if (message.key != null) estimatedBytes += Buffer.byteLength(message.key);
    if (message.value != null) estimatedBytes += Buffer.byteLength(message.value);
  }

  const recordBatch = await encodeRecordBatch({
    compression,
    compressionLevel,
    firstTimestamp,
    maxTimestamp,
    producerId,
    producerEpoch,
    firstSequence,
    transactional: transactionalId != null,
    lastOffsetDelta: messages.length - 1,
    recordCount: messages.length,
    estimatedBytes,
    writeRecords: (encoder) => {
      let offsetDelta = 0;
      for (const message of messages) {
        encodeRecord(
          {
            offsetDelta,
            timestampDelta: BigInt((message.timestamp ?? dateNow) - firstTimestamp),
            key: message.key ?? null,
            value: message.value ?? null,
            headers: message.headers ?? {},
          },
          encoder,
        );
        offsetDelta += 1;
      }
    },
  });

  return { partition, recordSet: recordBatch.buffer };
}

const flexibleRequestBodySchema = flexibleObject([
  field('transactionalId', compactNullableString),
  field('acks', int16),
  field('timeout', int32),
  field(
    'topicData',
    compactArray(
      flexibleObject([
        field('topic', compactString),
        field(
          'partitions',
          compactArray(flexibleObject([field('partition', int32), field('recordSet', compactBytes)])),
        ),
      ]),
    ),
  ),
]);

const flexibleRequestBodySchemaV13 = flexibleObject([
  field('transactionalId', compactNullableString),
  field('acks', int16),
  field('timeout', int32),
  field(
    'topicData',
    compactArray(
      flexibleObject([
        field('topicId', uuid),
        field(
          'partitions',
          compactArray(flexibleObject([field('partition', int32), field('recordSet', compactBytes)])),
        ),
      ]),
    ),
  ),
]);

async function encodeTopicPartitions(
  partitions: ProducePartitionData[],
  options: {
    compression: CompressionType;
    compressionLevel?: number;
    transactionalId?: string | null;
    producerId?: bigint;
    producerEpoch?: number;
  },
): Promise<{ partition: number; recordSet: Buffer }[]> {
  return Promise.all(partitions.map((partition) => encodePartition(partition, options)));
}

/**
 * Every version from 3 through 8 shares this exact request wire shape (KIP-98's RecordBatch v2
 * became mandatory at v3; each later version bump only signals a client capability — quota
 * timing in v6, ZSTD in v7, record-level errors in v8 — with no request field changes at all).
 * v9–v12 use the same fields with compact types and tagged fields (KIP-482). v13 replaces the
 * topic name with a topic UUID (KIP-516).
 */
export function createProduceRequest(apiVersion: number, options: ProduceRequestOptions): RequestDefinition {
  const { acks, timeout, transactionalId = null, producerId, producerEpoch, compressionLevel, topicData } = options;
  const compression = options.compression ?? COMPRESSION_TYPES.None;
  const partitionOptions = { compression, compressionLevel, transactionalId, producerId, producerEpoch };
  const schema = apiVersion >= 9 ? flexibleRequestBodySchema : requestBodySchema;

  return {
    apiKey: API_KEYS.Produce,
    apiVersion,
    apiName: 'Produce',
    // A request sent with acks=0 gets no response at all - the broker never writes one to the
    // wire, so the network layer must know not to wait for one.
    expectResponse: () => acks !== 0,
    encode: async () => {
      const encoder = new Encoder();

      if (apiVersion >= PRODUCE_TOPIC_ID_MIN_VERSION) {
        const encodedTopicData = await Promise.all(
          topicData.map(async ({ topic, topicId, partitions }) => {
            if (!isUsableTopicId(topicId)) {
              throw new RangeError(`Produce v${apiVersion} requires a 16-byte topicId for topic ${topic}`);
            }
            return { topicId, partitions: await encodeTopicPartitions(partitions, partitionOptions) };
          }),
        );
        flexibleRequestBodySchemaV13.write(encoder, { transactionalId, acks, timeout, topicData: encodedTopicData });
        return encoder;
      }

      const encodedTopicData = await Promise.all(
        topicData.map(async ({ topic, partitions }) => ({
          topic,
          partitions: await encodeTopicPartitions(partitions, partitionOptions),
        })),
      );
      schema.write(encoder, { transactionalId, acks, timeout, topicData: encodedTopicData });
      return encoder;
    },
  };
}

export interface ProducePartitionResult {
  partition: number;
  errorCode: number;
  baseOffset: bigint;
  logAppendTime: bigint;
}

export interface ProduceTopicResult {
  topicName: string;
  topicId?: Buffer;
  partitions: ProducePartitionResult[];
}

/**
 * Produce v13 responses carry a topic UUID, not a name. Map back to the name from the request
 * (by id, then by index) so `RecordMetadata.topicName` stays populated.
 */
export function resolveProduceTopicName(
  topicId: Buffer,
  index: number,
  topicData: readonly ProduceTopicData[],
): string {
  const byId = topicData.find((entry) => entry.topicId != null && entry.topicId.equals(topicId));
  if (byId) return byId.topic;
  const byIndex = topicData[index];
  return byIndex?.topic ?? '';
}

export interface ProduceResponseV3Body {
  topics: ProduceTopicResult[];
  throttleTime: number;
}

const responseBodySchema = object([
  field(
    'topics',
    array(
      object([
        field('topicName', string),
        field(
          'partitions',
          array(
            object([
              field('partition', int32),
              field('errorCode', int16),
              field('baseOffset', int64),
              field('logAppendTime', int64),
            ]),
          ),
        ),
      ]),
    ),
  ),
  field('throttleTime', int32),
]);

export interface LeaderIdAndEpoch {
  leaderId: number;
  leaderEpoch: number;
}

export interface ProduceNodeEndpoint {
  nodeId: number;
  host: string;
  port: number;
  rack: string | null;
}

const produceNodeEndpointSchema = flexibleObject([
  field('nodeId', int32),
  field('host', compactString),
  field('port', int32),
  field('rack', compactNullableString),
]);
const produceNodeEndpointsArraySchema = compactArray(produceNodeEndpointSchema);

/**
 * Reads a Produce partition's trailing tagged fields: CurrentLeader (tag 0, v10+, KIP-951).
 * Each tag's value is `tag:uvarint, size:uvarint, <size> bytes` (KIP-482) — not the
 * compact-bytes `N+1` framing `Decoder#readTaggedFields` uses for its blanket skip.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export function readProducePartitionTaggedFields(decoder: Decoder): LeaderIdAndEpoch | null {
  let currentLeader: LeaderIdAndEpoch | null = null;
  const numberOfTaggedFields = decoder.readUVarInt();

  for (let i = 0; i < numberOfTaggedFields; i++) {
    const tag = decoder.readUVarInt();
    const size = decoder.readUVarInt();
    const fieldDecoder = decoder.slice(size);
    decoder.forward(size);

    if (tag === 0) {
      currentLeader = { leaderId: fieldDecoder.readInt32(), leaderEpoch: fieldDecoder.readInt32() };
    }
  }

  return currentLeader;
}

/**
 * Reads the Produce response's trailing tagged fields: NodeEndpoints (tag 0, v10+, KIP-951) —
 * the broker addresses for any `currentLeader.leaderId` values in the response the client may
 * not already have cached.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export function readProduceResponseNodeEndpoints(decoder: Decoder): ProduceNodeEndpoint[] {
  let nodeEndpoints: ProduceNodeEndpoint[] = [];
  const numberOfTaggedFields = decoder.readUVarInt();

  for (let i = 0; i < numberOfTaggedFields; i++) {
    const tag = decoder.readUVarInt();
    const size = decoder.readUVarInt();
    const fieldDecoder = decoder.slice(size);
    decoder.forward(size);

    if (tag === 0) {
      nodeEndpoints = produceNodeEndpointsArraySchema.read(fieldDecoder);
    }
  }

  return nodeEndpoints;
}

/**
 * Shared by every Produce response version: throw the first partition-level failure found,
 * scanning topics in wire order.
 */
export async function parseProduceResponse<
  T extends {
    topics: readonly {
      topicName?: string;
      partitions: readonly { errorCode: number; partition?: number; currentLeader?: LeaderIdAndEpoch | null }[];
    }[];
    nodeEndpoints?: readonly ProduceNodeEndpoint[];
  },
>(data: T): Promise<T> {
  for (const topic of data.topics) {
    const firstError = topic.partitions.find((p) => failure(p.errorCode));
    if (firstError) {
      throw createErrorFromCode(firstError.errorCode, {
        topic: topic.topicName,
        partition: firstError.partition,
        currentLeader: firstError.currentLeader ?? undefined,
        nodeEndpoints: data.nodeEndpoints,
      });
    }
  }
  return data;
}

/** Response wire shape for v3-v4 - identical, so v4's response.ts re-exports this directly. */
export const produceResponseV3 = defineResponse({
  schema: responseBodySchema,
  parse: parseProduceResponse,
});
