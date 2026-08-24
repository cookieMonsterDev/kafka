import { Decoder } from '../../../decoder';
import { uuid, type ResponseDefinition } from '../../../schema';
import {
  decodeCompactRecordSet,
  parseFetchResponse,
  readFetchPartitionTaggedFields,
  readFetchResponseNodeEndpoints,
  resolveFetchTopicName,
  type FetchRequestOptions,
} from '../shared';
import type { FetchPartitionResponseV11, FetchResponseV11Body, FetchTopicResponseV11 } from '../v11/response';

export type FetchPartitionResponseV13 = FetchPartitionResponseV11;

export interface FetchTopicResponseV13 extends FetchTopicResponseV11 {
  topicId: Buffer;
}

export interface FetchResponseV13Body extends FetchResponseV11Body {
  responses: FetchTopicResponseV13[];
}

async function readCompactArrayAsync<T>(decoder: Decoder, reader: (d: Decoder) => Promise<T>): Promise<T[]> {
  const encodedLength = decoder.readUVarInt();
  if (encodedLength === 0) return [];
  const length = encodedLength - 1;
  const values = new Array<T>(length);
  for (let i = 0; i < length; i++) values[i] = await reader(decoder);
  return values;
}

async function decodePartition(decoder: Decoder): Promise<FetchPartitionResponseV13> {
  const partition = decoder.readInt32();
  const errorCode = decoder.readInt16();
  const highWatermark = decoder.readInt64();
  const lastStableOffset = decoder.readInt64();
  const logStartOffset = decoder.readInt64();
  const abortedTransactions =
    decoder.readUVarIntArray((d) => {
      const txn = { producerId: d.readInt64(), firstOffset: d.readInt64() };
      d.readTaggedFields();
      return txn;
    }) ?? [];
  const preferredReadReplica = decoder.readInt32();
  const messages = await decodeCompactRecordSet(decoder);
  const currentLeader = readFetchPartitionTaggedFields(decoder);
  return {
    partition,
    errorCode,
    highWatermark,
    lastStableOffset,
    logStartOffset,
    abortedTransactions,
    preferredReadReplica,
    messages,
    currentLeader,
  };
}

/**
 * Fetch Response (Version: 13+) => throttle_time_ms error_code session_id [responses] TAG_BUFFER
 *   responses => topic_id [partitions] TAG_BUFFER
 *     topic_id => UUID
 *
 * Topic names are replaced with topic IDs (KIP-516). `decode` restores `topicName` from the
 * request so consumers stay name-based. DivergingEpoch (tag 0) and SnapshotId (tag 2, v12+)
 * stay skipped; CurrentLeader (tag 1, v12+) and NodeEndpoints (v16+) are decoded (KIP-951).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export function fetchResponseV13(
  options: Pick<FetchRequestOptions, 'topics' | 'topicsForResponse'> = { topics: [] },
): ResponseDefinition<FetchResponseV13Body> {
  const resolutionTopics = options.topicsForResponse ?? options.topics;
  return {
    decode: async (rawData) => {
      const decoder = new Decoder(rawData);
      const clientSideThrottleTime = decoder.readInt32();
      const errorCode = decoder.readInt16();
      const sessionId = decoder.readInt32();
      const responses = await readCompactArrayAsync(decoder, async (d) => {
        const topicId = uuid.read(d);
        const partitions = await readCompactArrayAsync(d, decodePartition);
        d.readTaggedFields();
        return { topicId, partitions };
      });
      const nodeEndpoints = readFetchResponseNodeEndpoints(decoder);
      return {
        throttleTime: 0,
        clientSideThrottleTime,
        errorCode,
        sessionId,
        responses: responses.map((topic, index) => ({
          topicName: resolveFetchTopicName(topic.topicId, index, resolutionTopics),
          topicId: topic.topicId,
          partitions: topic.partitions,
        })),
        nodeEndpoints,
      };
    },
    parse: parseFetchResponse,
  };
}
