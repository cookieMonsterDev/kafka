import { array, defineRequest, field, int32, int64, nullableString, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * OffsetCommit Request (Version: 0) => group_id [topics]
 *   group_id => STRING
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition offset metadata
 *       partition => INT32
 *       offset => INT64
 *       metadata => NULLABLE_STRING
 */
const partitionSchema = object([field('partition', int32), field('offset', int64), field('metadata', nullableString)]);
const topicSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const requestSchema = object([field('groupId', string), field('topics', array(topicSchema))]);

export const offsetCommitRequestV0 = defineRequest({
  apiKey: API_KEYS.OffsetCommit,
  apiVersion: 0,
  apiName: 'OffsetCommit',
  schema: requestSchema,
});
