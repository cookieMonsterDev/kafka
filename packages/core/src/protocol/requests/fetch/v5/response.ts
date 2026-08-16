import { Decoder } from '../../../decoder';
import type { ResponseDefinition } from '../../../schema';
import type { DecodedRecordBatch } from '../../../records/batch';
import { decodeRecordSet, parseFetchResponse, readTopicName } from '../shared';

export interface FetchPartitionResponseV5 {
  partition: number;
  errorCode: number;
  highWatermark: bigint;
  lastStableOffset: bigint;
  logStartOffset: bigint;
  abortedTransactions: { producerId: bigint; firstOffset: bigint }[];
  messages: DecodedRecordBatch['records'];
}

export interface FetchTopicResponseV5 {
  topicName: string;
  partitions: FetchPartitionResponseV5[];
}

export interface FetchResponseV5Body {
  throttleTime: number;
  responses: FetchTopicResponseV5[];
}

async function decodePartition(decoder: Decoder): Promise<FetchPartitionResponseV5> {
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

async function decodeTopicResponse(decoder: Decoder): Promise<FetchTopicResponseV5> {
  return { topicName: readTopicName(decoder), partitions: await decoder.readArrayAsync(decodePartition) };
}

/**
 * Fetch Response (Version: 5) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition_header record_set
 *       partition_header => partition error_code high_watermark last_stable_offset log_start_offset [aborted_transactions]
 *         partition => INT32
 *         error_code => INT16
 *         high_watermark => INT64
 *         last_stable_offset => INT64
 *         log_start_offset => INT64
 *         aborted_transactions => producer_id first_offset
 *           producer_id => INT64
 *           first_offset => INT64
 *       record_set => RECORDS
 */
export const fetchResponseV5: ResponseDefinition<FetchResponseV5Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const throttleTime = decoder.readInt32();
    const responses = await decoder.readArrayAsync(decodeTopicResponse);
    return { throttleTime, responses };
  },
  parse: parseFetchResponse,
};
