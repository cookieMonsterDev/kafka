import { array, defineRequest, field, int32, nullableArray, object, string } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';

/**
 * OffsetFetch Request (Version: 4) => group_id [topics]
 *   group_id => STRING
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition
 *       partition => INT32
 *
 * Wire format is identical to v3.
 */
const partitionSchema = object([field('partition', int32)]);
const topicSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const requestSchema = object([field('groupId', string), field('topics', nullableArray(topicSchema))]);

export const offsetFetchRequestV4 = defineRequest({
  apiKey: API_KEYS.OffsetFetch,
  apiVersion: 4,
  apiName: 'OffsetFetch',
  schema: requestSchema,
});
