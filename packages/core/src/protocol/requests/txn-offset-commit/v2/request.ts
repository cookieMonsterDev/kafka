import { array, defineRequest, field, int16, int32, int64, nullableString, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * TxnOffsetCommit Request (Version: 2) => transactional_id group_id producer_id producer_epoch [topics]
 *   transactional_id => STRING
 *   group_id => STRING
 *   producer_id => INT64
 *   producer_epoch => INT16
 *   topics => name [partitions]
 *     name => STRING
 *     partitions => partition_index committed_offset committed_leader_epoch committed_metadata
 *       partition_index => INT32
 *       committed_offset => INT64
 *       committed_leader_epoch => INT32
 *       committed_metadata => NULLABLE_STRING
 *
 * Adds `leaderEpoch` on each partition (default -1).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = object([
  field('partition', int32),
  field('offset', int64),
  field('leaderEpoch', int32),
  field('metadata', nullableString),
]);
export const requestSchema = object([
  field('transactionalId', string),
  field('groupId', string),
  field('producerId', int64),
  field('producerEpoch', int16),
  field('topics', array(object([field('topic', string), field('partitions', array(partitionSchema))]))),
]);

export const txnOffsetCommitRequestV2 = defineRequest({
  apiKey: API_KEYS.TxnOffsetCommit,
  apiVersion: 2,
  apiName: 'TxnOffsetCommit',
  schema: requestSchema,
});
