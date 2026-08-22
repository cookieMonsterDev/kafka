import {
  boolean,
  compactArray,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int16,
  int32,
  int64,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface WriteTxnMarkersTopic {
  topic: string;
  partitions: number[];
}

export interface WriteTxnMarkersMarkerV1 {
  producerId: bigint;
  producerEpoch: number;
  transactionResult: boolean;
  coordinatorEpoch: number;
  topics: WriteTxnMarkersTopic[];
}

export interface WriteTxnMarkersRequestV1Fields {
  markers: WriteTxnMarkersMarkerV1[];
}

/**
 * WriteTxnMarkers Request (Version: 1) => [markers] TAG_BUFFER
 *   markers => producer_id producer_epoch transaction_result [topics] coordinator_epoch TAG_BUFFER
 *     producer_id => INT64
 *     producer_epoch => INT16
 *     transaction_result => BOOLEAN
 *     topics => name [partition_indexes] TAG_BUFFER
 *       name => COMPACT_STRING
 *       partition_indexes => INT32
 *     coordinator_epoch => INT32
 *
 * Flexible from v1. v0 was removed in Kafka 4.0.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(int32))]);

const markerSchema = flexibleObject([
  field('producerId', int64),
  field('producerEpoch', int16),
  field('transactionResult', boolean),
  field('topics', compactArray(topicSchema)),
  field('coordinatorEpoch', int32),
]);

export const requestSchema = flexibleObject([field('markers', compactArray(markerSchema))]);

export const writeTxnMarkersRequestV1 = defineRequest({
  apiKey: API_KEYS.WriteTxnMarkers,
  apiVersion: 1,
  apiName: 'WriteTxnMarkers',
  schema: requestSchema,
});
