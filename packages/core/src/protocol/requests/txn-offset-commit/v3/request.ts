import {
  compactArray,
  compactNullableString,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int16,
  int32,
  int64,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * TxnOffsetCommit Request (Version: 3) => transactional_id group_id producer_id producer_epoch
 *                                         generation_id member_id group_instance_id [topics] TAG_BUFFER
 *   transactional_id => COMPACT_STRING
 *   group_id => COMPACT_STRING
 *   producer_id => INT64
 *   producer_epoch => INT16
 *   generation_id => INT32
 *   member_id => COMPACT_STRING
 *   group_instance_id => COMPACT_NULLABLE_STRING
 *   topics => name [partitions] TAG_BUFFER
 *     name => COMPACT_STRING
 *     partitions => partition_index committed_offset committed_leader_epoch committed_metadata TAG_BUFFER
 *       partition_index => INT32
 *       committed_offset => INT64
 *       committed_leader_epoch => INT32
 *       committed_metadata => COMPACT_NULLABLE_STRING
 *
 * First flexible version (KIP-482). Adds generationId, memberId, and groupInstanceId.
 * Request header v2's trailing TAG_BUFFER is written by `createRequest`, not here.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = flexibleObject([
  field('partition', int32),
  field('offset', int64),
  field('leaderEpoch', int32),
  field('metadata', compactNullableString),
]);
const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(partitionSchema))]);
export const requestSchema = flexibleObject([
  field('transactionalId', compactString),
  field('groupId', compactString),
  field('producerId', int64),
  field('producerEpoch', int16),
  field('generationId', int32),
  field('memberId', compactString),
  field('groupInstanceId', compactNullableString),
  field('topics', compactArray(topicSchema)),
]);

export const txnOffsetCommitRequestV3 = defineRequest({
  apiKey: API_KEYS.TxnOffsetCommit,
  apiVersion: 3,
  apiName: 'TxnOffsetCommit',
  schema: requestSchema,
});
