import { array, defineRequest, field, int32, int64, nullableString, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * Version 5 removes retention_time — it is now controlled entirely by a broker-side setting.
 *
 * OffsetCommit Request (Version: 5) => group_id generation_id member_id [topics]
 *   group_id => STRING
 *   generation_id => INT32
 *   member_id => STRING
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition offset metadata
 *       partition => INT32
 *       offset => INT64
 *       metadata => NULLABLE_STRING
 */
const partitionSchema = object([field('partition', int32), field('offset', int64), field('metadata', nullableString)]);
const topicSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const requestSchema = object([
  field('groupId', string),
  field('groupGenerationId', int32),
  field('memberId', string),
  field('topics', array(topicSchema)),
]);

export const offsetCommitRequestV5 = defineRequest({
  apiKey: API_KEYS.OffsetCommit,
  apiVersion: 5,
  apiName: 'OffsetCommit',
  schema: requestSchema,
});
