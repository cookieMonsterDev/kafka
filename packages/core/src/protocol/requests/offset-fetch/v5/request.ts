import { array, defineRequest, field, int32, nullableArray, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * OffsetFetch Request (Version: 5) => group_id [topics]
 *   group_id => STRING
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition
 *       partition => INT32
 *
 * Wire format is identical to v4.
 */
const partitionSchema = object([field('partition', int32)]);
const topicSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
export const requestSchema = object([field('groupId', string), field('topics', nullableArray(topicSchema))]);

export const offsetFetchRequestV5 = defineRequest({
  apiKey: API_KEYS.OffsetFetch,
  apiVersion: 5,
  apiName: 'OffsetFetch',
  schema: requestSchema,
});
