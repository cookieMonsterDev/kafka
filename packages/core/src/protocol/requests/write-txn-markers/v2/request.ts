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
  int8,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';
import type { WriteTxnMarkersMarkerV1, WriteTxnMarkersTopic } from '../v1/request';

export type { WriteTxnMarkersTopic };

export interface WriteTxnMarkersMarkerV2 extends WriteTxnMarkersMarkerV1 {
  transactionVersion: number;
}

export interface WriteTxnMarkersRequestV2Fields {
  markers: WriteTxnMarkersMarkerV2[];
}

/**
 * WriteTxnMarkers Request (Version: 2) => [markers] TAG_BUFFER
 *   markers => producer_id producer_epoch transaction_result [topics] coordinator_epoch
 *              transaction_version TAG_BUFFER
 *     transaction_version => INT8
 *
 * Adds `transactionVersion` (KIP-1228) for stricter epoch validation on partition leaders.
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
  field('transactionVersion', int8),
]);

export const requestSchema = flexibleObject([field('markers', compactArray(markerSchema))]);

export const writeTxnMarkersRequestV2 = defineRequest({
  apiKey: API_KEYS.WriteTxnMarkers,
  apiVersion: 2,
  apiName: 'WriteTxnMarkers',
  schema: requestSchema,
});
