import { Decoder } from '../../../decoder.js';
import type { ResponseDefinition } from '../../../schema.js';
import { decodeRecordSet, parseFetchResponse, readTopicName } from '../shared.js';
import type { DecodedRecordBatch } from '../../../records/batch.js';

export interface FetchPartitionResponseV7 {
  partition: number;
  errorCode: number;
  highWatermark: bigint;
  lastStableOffset: bigint;
  logStartOffset: bigint;
  abortedTransactions: { producerId: bigint; firstOffset: bigint }[];
  messages: DecodedRecordBatch['records'];
}

export interface FetchTopicResponseV7 {
  topicName: string;
  partitions: FetchPartitionResponseV7[];
}

export interface FetchResponseV7Body {
  throttleTime: number;
  errorCode: number;
  sessionId: number;
  responses: FetchTopicResponseV7[];
}

async function decodePartition(decoder: Decoder): Promise<FetchPartitionResponseV7> {
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
    messages: await decodeRecordSet(decoder),
  };
}

async function decodeTopicResponse(decoder: Decoder): Promise<FetchTopicResponseV7> {
  return { topicName: readTopicName(decoder), partitions: await decoder.readArrayAsync(decodePartition) };
}

/**
 * Fetch Response (Version: 7) => throttle_time_ms error_code session_id [responses]
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   session_id => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition_header record_set
 *       partition_header => partition error_code high_watermark last_stable_offset log_start_offset [aborted_transactions]
 *       record_set => RECORDS
 */
export const fetchResponseV7: ResponseDefinition<FetchResponseV7Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const throttleTime = decoder.readInt32();
    const errorCode = decoder.readInt16();
    const sessionId = decoder.readInt32();
    const responses = await decoder.readArrayAsync(decodeTopicResponse);
    return { throttleTime, errorCode, sessionId, responses };
  },
  parse: parseFetchResponse,
};
