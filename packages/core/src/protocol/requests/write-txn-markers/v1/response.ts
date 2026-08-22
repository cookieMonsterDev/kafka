import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { compactArray, compactString, field, flexibleObject, int16, int32, int64 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface WriteTxnMarkersPartitionResult {
  partition: number;
  errorCode: number;
}

export interface WriteTxnMarkersTopicResult {
  topic: string;
  partitions: WriteTxnMarkersPartitionResult[];
}

export interface WriteTxnMarkersMarkerResult {
  producerId: bigint;
  topics: WriteTxnMarkersTopicResult[];
}

export interface WriteTxnMarkersResponseV1Body {
  markers: WriteTxnMarkersMarkerResult[];
}

/**
 * WriteTxnMarkers Response (Version: 1) => [markers] TAG_BUFFER
 *   markers => producer_id [topics] TAG_BUFFER
 *     producer_id => INT64
 *     topics => name [partitions] TAG_BUFFER
 *       name => COMPACT_STRING
 *       partitions => partition_index error_code TAG_BUFFER
 *         partition_index => INT32
 *         error_code => INT16
 *
 * v2 uses the same response wire format.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = flexibleObject([field('partition', int32), field('errorCode', int16)]);

const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(partitionSchema))]);

const markerSchema = flexibleObject([field('producerId', int64), field('topics', compactArray(topicSchema))]);

export const responseSchema = flexibleObject([field('markers', compactArray(markerSchema))]);

export function throwOnWriteTxnMarkersPartitionErrors(markers: WriteTxnMarkersMarkerResult[]): void {
  const partitionWithError = markers
    .flatMap((marker) => marker.topics)
    .flatMap((topic) => topic.partitions)
    .find((partition) => failure(partition.errorCode));
  if (partitionWithError) throw createErrorFromCode(partitionWithError.errorCode);
}

export const writeTxnMarkersResponseV1: ResponseDefinition<WriteTxnMarkersResponseV1Body> = {
  decode: async (rawData) => responseSchema.read(new Decoder(rawData)),
  parse: async (data) => {
    throwOnWriteTxnMarkersPartitionErrors(data.markers);
    return data;
  },
};
