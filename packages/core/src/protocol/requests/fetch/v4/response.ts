import { Decoder } from '../../../decoder';
import type { ResponseDefinition } from '../../../schema';
import type { DecodedRecordBatch } from '../../../records/batch';
import { decodeRecordSet, parseFetchResponse, readTopicName, type FetchRequestOptions } from '../shared';

export interface FetchPartitionResponseV4 {
  partition: number;
  errorCode: number;
  highWatermark: bigint;
  lastStableOffset: bigint;
  abortedTransactions: { producerId: bigint; firstOffset: bigint }[];
  messages: DecodedRecordBatch['records'];
}

export interface FetchTopicResponseV4 {
  topicName: string;
  partitions: FetchPartitionResponseV4[];
}

export interface FetchResponseV4Body {
  throttleTime: number;
  responses: FetchTopicResponseV4[];
}

function decodePartition(checkCrcs?: boolean) {
  return async (decoder: Decoder): Promise<FetchPartitionResponseV4> => ({
    partition: decoder.readInt32(),
    errorCode: decoder.readInt16(),
    highWatermark: decoder.readInt64(),
    lastStableOffset: decoder.readInt64(),
    abortedTransactions: decoder.readArray(() => ({
      producerId: decoder.readInt64(),
      firstOffset: decoder.readInt64(),
    })),
    messages: await decodeRecordSet(decoder, checkCrcs),
  });
}

function decodeTopicResponse(checkCrcs?: boolean) {
  return async (decoder: Decoder): Promise<FetchTopicResponseV4> => ({
    topicName: readTopicName(decoder),
    partitions: await decoder.readArrayAsync(decodePartition(checkCrcs)),
  });
}

/**
 * Fetch Response (Version: 4) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition_header record_set
 *       partition_header => partition error_code high_watermark last_stable_offset [aborted_transactions]
 *         partition => INT32
 *         error_code => INT16
 *         high_watermark => INT64
 *         last_stable_offset => INT64
 *         aborted_transactions => producer_id first_offset
 *           producer_id => INT64
 *           first_offset => INT64
 *       record_set => RECORDS
 *
 * Hand-written rather than schema-driven: `record_set` requires the async, compression-aware,
 * loop-until-partial decode in `decodeRecordSet`, which the synchronous `FieldCodec` shape can't
 * express.
 */
export function fetchResponseV4(
  options: Pick<FetchRequestOptions, 'checkCrcs'> = {},
): ResponseDefinition<FetchResponseV4Body> {
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
