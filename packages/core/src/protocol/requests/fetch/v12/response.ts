import { Decoder } from '../../../decoder';
import type { ResponseDefinition } from '../../../schema';
import {
  decodeCompactRecordSet,
  parseFetchResponse,
  readFetchPartitionTaggedFields,
  readFetchResponseNodeEndpoints,
} from '../shared';
import type { FetchPartitionResponseV11, FetchResponseV11Body, FetchTopicResponseV11 } from '../v11/response';

export type FetchPartitionResponseV12 = FetchPartitionResponseV11;
export type FetchTopicResponseV12 = FetchTopicResponseV11;
export type FetchResponseV12Body = FetchResponseV11Body;

async function readCompactArrayAsync<T>(decoder: Decoder, reader: (d: Decoder) => Promise<T>): Promise<T[]> {
  const encodedLength = decoder.readUVarInt();
  if (encodedLength === 0) return [];
  const length = encodedLength - 1;
  const values = new Array<T>(length);
  for (let i = 0; i < length; i++) values[i] = await reader(decoder);
  return values;
}

async function decodePartition(decoder: Decoder): Promise<FetchPartitionResponseV12> {
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

async function decodeTopicResponse(decoder: Decoder): Promise<FetchTopicResponseV12> {
  const topicName = decoder.readUVarIntString();
  if (topicName === null) throw new RangeError('Expected a non-null topic name, got null');
  const partitions = await readCompactArrayAsync(decoder, decodePartition);
  decoder.readTaggedFields();
  return { topicName, partitions };
}

/**
 * Fetch Response (Version: 12) => throttle_time_ms error_code session_id [responses] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   session_id => INT32
 *   responses => topic [partitions] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partitions => partition_index error_code high_watermark last_stable_offset log_start_offset
 *                   [aborted_transactions] preferred_read_replica records TAG_BUFFER
 *       records => COMPACT_RECORDS
 *
 * First flexible Fetch response. DivergingEpoch (tag 0) and SnapshotId (tag 2) are tagged
 * fields that stay skipped; CurrentLeader (tag 1, KIP-951) is decoded. Topic names remain
 * through v12.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const fetchResponseV12: ResponseDefinition<FetchResponseV12Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const clientSideThrottleTime = decoder.readInt32();
    const errorCode = decoder.readInt16();
    const sessionId = decoder.readInt32();
    const responses = await readCompactArrayAsync(decoder, decodeTopicResponse);
    const nodeEndpoints = readFetchResponseNodeEndpoints(decoder);
    return { throttleTime: 0, clientSideThrottleTime, errorCode, sessionId, responses, nodeEndpoints };
  },
  parse: parseFetchResponse,
};
