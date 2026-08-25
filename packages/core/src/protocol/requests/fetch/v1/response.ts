import { Decoder } from '../../../decoder';
import { decodeMessageSet } from '../../../message-set/decoder';
import type { ResponseDefinition } from '../../../schema';
import type { DecodedMessageSetRecord } from '../../../message-set/decoder';
import { parseFetchResponse, readTopicName, type FetchRequestOptions } from '../shared';

export interface FetchPartitionResponseV1 {
  partition: number;
  errorCode: number;
  highWatermark: bigint;
  messages: DecodedMessageSetRecord[];
}

export interface FetchTopicResponseV1 {
  topicName: string;
  partitions: FetchPartitionResponseV1[];
}

export interface FetchResponseV1Body {
  throttleTime: number;
  responses: FetchTopicResponseV1[];
}

function decodePartition(checkCrcs?: boolean) {
  return async (decoder: Decoder): Promise<FetchPartitionResponseV1> => ({
    partition: decoder.readInt32(),
    errorCode: decoder.readInt16(),
    highWatermark: decoder.readInt64(),
    messages: await decodeMessageSet(decoder, undefined, checkCrcs),
  });
}

function decodeTopicResponse(checkCrcs?: boolean) {
  return async (decoder: Decoder): Promise<FetchTopicResponseV1> => ({
    topicName: readTopicName(decoder),
    partitions: await decoder.readArrayAsync(decodePartition(checkCrcs)),
  });
}

/**
 * Fetch Response (Version: 1) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition_header record_set
 *       partition_header => partition error_code high_watermark
 *         partition => INT32
 *         error_code => INT16
 *         high_watermark => INT64
 *       record_set => RECORDS
 */
export function fetchResponseV1(
  options: Pick<FetchRequestOptions, 'checkCrcs'> = {},
): ResponseDefinition<FetchResponseV1Body> {
  return {
    decode: async (rawData) => {
      const decoder = new Decoder(rawData);
      const throttleTime = decoder.readInt32();
      const responses = await decoder.readArrayAsync(decodeTopicResponse(options.checkCrcs));
      return { throttleTime, responses };
    },
    parse: parseFetchResponse,
  };
}
