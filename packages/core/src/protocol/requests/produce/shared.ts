import { encodeRecord, type RecordHeaders } from '../../records/record';
import { encodeRecordBatch } from '../../records/batch';
import { COMPRESSION_TYPES, type CompressionType } from '../../compression/index';
import { Encoder } from '../../encoder';
import { createErrorFromCode, failure } from '../../error-codes';
import {
  array,
  bytes,
  defineResponse,
  field,
  int16,
  int32,
  int64,
  nullableString,
  object,
  string,
  type RequestDefinition,
} from '../../schema';
import { API_KEYS } from '../api-keys';

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
  partitions: ProducePartitionData[];
}

export interface ProduceRequestOptions {
  acks: number;
  timeout: number;
  transactionalId?: string | null;
  producerId?: bigint;
  producerEpoch?: number;
  compression?: CompressionType;
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
    transactionalId,
    producerId,
    producerEpoch,
  }: { compression: CompressionType; transactionalId?: string | null; producerId?: bigint; producerEpoch?: number },
): Promise<{ partition: number; recordSet: Buffer }> {
  const dateNow = Date.now();
  const messageTimestamps = messages
    .map((m) => m.timestamp)
    .filter((timestamp): timestamp is number => timestamp != null);

  const firstTimestamp = messageTimestamps.length === 0 ? dateNow : Math.min(...messageTimestamps);
  const maxTimestamp = messageTimestamps.length === 0 ? dateNow : Math.max(...messageTimestamps);

  const records = messages.map((message, i) =>
    encodeRecord({
      offsetDelta: i,
      timestampDelta: BigInt((message.timestamp ?? dateNow) - firstTimestamp),
      key: message.key ?? null,
      value: message.value ?? null,
      headers: message.headers ?? {},
    }),
  );

  const recordBatch = await encodeRecordBatch({
    compression,
    records,
    firstTimestamp,
    maxTimestamp,
    producerId,
    producerEpoch,
    firstSequence,
    transactional: transactionalId != null,
    lastOffsetDelta: records.length - 1,
  });

  return { partition, recordSet: recordBatch.buffer };
}

/**
 * Every version from 3 through 7 shares this exact request wire shape (KIP-98's RecordBatch v2
 * became mandatory at v3; each later version bump only signals a client capability — quota
 * timing in v6, ZSTD in v7 — with no request field changes at all).
 */
export function createProduceRequest(apiVersion: number, options: ProduceRequestOptions): RequestDefinition {
  const { acks, timeout, transactionalId = null, producerId, producerEpoch, topicData } = options;
  const compression = options.compression ?? COMPRESSION_TYPES.None;

  return {
    apiKey: API_KEYS.Produce,
    apiVersion,
    apiName: 'Produce',
    // A request sent with acks=0 gets no response at all - the broker never writes one to the
    // wire, so the network layer must know not to wait for one.
    expectResponse: () => acks !== 0,
    encode: async () => {
      const encodedTopicData = await Promise.all(
        topicData.map(async ({ topic, partitions }) => ({
          topic,
          partitions: await Promise.all(
            partitions.map((partition) =>
              encodePartition(partition, { compression, transactionalId, producerId, producerEpoch }),
            ),
          ),
        })),
      );

      const encoder = new Encoder();
      requestBodySchema.write(encoder, { transactionalId, acks, timeout, topicData: encodedTopicData });
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
  partitions: ProducePartitionResult[];
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

/**
 * Shared by every Produce response version: throw the first partition-level failure found,
 * scanning topics in wire order.
 */
export async function parseProduceResponse<
  T extends {
    topics: readonly { topicName?: string; partitions: readonly { errorCode: number; partition?: number }[] }[];
  },
>(data: T): Promise<T> {
  for (const topic of data.topics) {
    const firstError = topic.partitions.find((p) => failure(p.errorCode));
    if (firstError) {
      throw createErrorFromCode(firstError.errorCode, { topic: topic.topicName, partition: firstError.partition });
    }
  }
  return data;
}

/** Response wire shape for v3-v4 - identical, so v4's response.ts re-exports this directly. */
export const produceResponseV3 = defineResponse({
  schema: responseBodySchema,
  parse: parseProduceResponse,
});
