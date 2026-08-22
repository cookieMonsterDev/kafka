import { Decoder } from '../../../decoder';
import type { ResponseDefinition } from '../../../schema';
import { decodeRecordSet, parseFetchResponse, readTopicName } from '../shared';
import type { DecodedRecordBatch } from '../../../records/batch';

export interface FetchPartitionResponseV11 {
  partition: number;
  errorCode: number;
  highWatermark: bigint;
  lastStableOffset: bigint;
  logStartOffset: bigint;
  abortedTransactions: { producerId: bigint; firstOffset: bigint }[];
  preferredReadReplica: number;
  messages: DecodedRecordBatch['records'];
}

export interface FetchTopicResponseV11 {
  topicName: string;
  /** KIP-516 topic UUID; present on Fetch v13+ responses. */
  topicId?: Buffer;
  partitions: FetchPartitionResponseV11[];
}

export interface FetchResponseV11Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  errorCode: number;
  sessionId: number;
  responses: FetchTopicResponseV11[];
}

async function decodePartition(decoder: Decoder): Promise<FetchPartitionResponseV11> {
  return {
    partition: decoder.readInt32(),
    errorCode: decoder.readInt16(),
    highWatermark: decoder.readInt64(),
    lastStableOffset: decoder.readInt64(),
    logStartOffset: decoder.readInt64(),
    abortedTransactions: decoder.readArray(() => ({
      producerId: decoder.readInt64(),
      firstOffset: decoder.readInt64(),
    })),
    preferredReadReplica: decoder.readInt32(),
    messages: await decodeRecordSet(decoder),
  };
}

async function decodeTopicResponse(decoder: Decoder): Promise<FetchTopicResponseV11> {
  return { topicName: readTopicName(decoder), partitions: await decoder.readArrayAsync(decodePartition) };
}

/**
 * Allow consumers to fetch from the closest replica (KIP-392): each partition now reports the
 * broker it would prefer the client fetch from next.
 *
 * Fetch Response (Version: 11) => throttle_time_ms error_code session_id [responses]
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   session_id => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition_header record_set
 *       partition_header => partition error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica
 *       record_set => RECORDS
 *
 * As with every family since KIP-219 (v6+ here), the broker sends the response before
 * throttling: report a client-facing `throttleTime` of 0 and surface the wire value as
 * `clientSideThrottleTime` for the caller to act on.
 */
export const fetchResponseV11: ResponseDefinition<FetchResponseV11Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const clientSideThrottleTime = decoder.readInt32();
    const errorCode = decoder.readInt16();
    const sessionId = decoder.readInt32();
    const responses = await decoder.readArrayAsync(decodeTopicResponse);
    return { throttleTime: 0, clientSideThrottleTime, errorCode, sessionId, responses };
  },
  parse: parseFetchResponse,
};
