import { Decoder } from '../../../decoder';
import { decodeMessageSet } from '../../../message-set/decoder';
import type { ResponseDefinition } from '../../../schema';
import type { DecodedMessageSetRecord } from '../../../message-set/decoder';
import { parseFetchResponse, readTopicName } from '../shared';

export interface FetchPartitionResponseV0 {
  partition: number;
  errorCode: number;
  highWatermark: bigint;
  messages: DecodedMessageSetRecord[];
}

export interface FetchTopicResponseV0 {
  topicName: string;
  partitions: FetchPartitionResponseV0[];
}

export interface FetchResponseV0Body {
  responses: FetchTopicResponseV0[];
}

async function decodePartition(decoder: Decoder): Promise<FetchPartitionResponseV0> {
  return {
    partition: decoder.readInt32(),
    errorCode: decoder.readInt16(),
    highWatermark: decoder.readInt64(),
    messages: await decodeMessageSet(decoder),
  };
}

async function decodeTopicResponse(decoder: Decoder): Promise<FetchTopicResponseV0> {
  return { topicName: readTopicName(decoder), partitions: await decoder.readArrayAsync(decodePartition) };
}

/**
 * Fetch Response (Version: 0) => [responses]
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition_header record_set
 *       partition_header => partition error_code high_watermark
 *         partition => INT32
 *         error_code => INT16
 *         high_watermark => INT64
 *       record_set => RECORDS
 */
export const fetchResponseV0: ResponseDefinition<FetchResponseV0Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const responses = await decoder.readArrayAsync(decodeTopicResponse);
    return { responses };
  },
  parse: parseFetchResponse,
};
