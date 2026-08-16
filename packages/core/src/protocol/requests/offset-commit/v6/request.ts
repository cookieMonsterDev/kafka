import { array, defineRequest, field, int32, int64, nullableString, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * Version 6 adds committed_leader_epoch for fencing (KIP-320).
 *
 * OffsetCommit Request (Version: 6) => group_id generation_id member_id [topics]
 *   group_id => STRING
 *   generation_id => INT32
 *   member_id => STRING
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition offset committed_leader_epoch metadata
 *       partition => INT32
 *       offset => INT64
 *       committed_leader_epoch => INT32
 *       metadata => NULLABLE_STRING
 */
const partitionSchema = object([
  field('partition', int32),
  field('offset', int64),
  field('leaderEpoch', int32),
  field('metadata', nullableString),
]);
const topicSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
export const requestSchema = object([
  field('groupId', string),
  field('groupGenerationId', int32),
  field('memberId', string),
  field('topics', array(topicSchema)),
]);

export const offsetCommitRequestV6 = defineRequest({
  apiKey: API_KEYS.OffsetCommit,
  apiVersion: 6,
  apiName: 'OffsetCommit',
  schema: requestSchema,
});
