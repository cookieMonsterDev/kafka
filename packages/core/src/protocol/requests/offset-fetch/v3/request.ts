import { array, defineRequest, field, int32, nullableArray, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * OffsetFetch Request (Version: 3) => group_id [topics]
 *   group_id => STRING
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition
 *       partition => INT32
 *
 * `topics` becomes a nullable array: an empty input collapses to wire length -1, meaning "fetch
 * offsets for every topic the group is subscribed to" rather than an empty result.
 */
const partitionSchema = object([field('partition', int32)]);
const topicSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const requestSchema = object([field('groupId', string), field('topics', nullableArray(topicSchema))]);

export const offsetFetchRequestV3 = defineRequest({
  apiKey: API_KEYS.OffsetFetch,
  apiVersion: 3,
  apiName: 'OffsetFetch',
  schema: requestSchema,
});
