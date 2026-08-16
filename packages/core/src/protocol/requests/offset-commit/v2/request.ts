import { array, defineRequest, field, int32, int64, nullableString, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * OffsetCommit Request (Version: 2) => group_id group_generation_id member_id retention_time [topics]
 *   group_id => STRING
 *   group_generation_id => INT32
 *   member_id => STRING
 *   retention_time => INT64
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
  field('retentionTime', int64),
  field('topics', array(topicSchema)),
]);

export const offsetCommitRequestV2 = defineRequest({
  apiKey: API_KEYS.OffsetCommit,
  apiVersion: 2,
  apiName: 'OffsetCommit',
  schema: requestSchema,
});
