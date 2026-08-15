import { array, defineRequest, field, int32, object, string } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';

/**
 * OffsetFetch Request (Version: 2) => group_id [topics]
 *   group_id => STRING
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition
 *       partition => INT32
 *
 * Wire format is identical to v1.
 */
const partitionSchema = object([field('partition', int32)]);
const topicSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const requestSchema = object([field('groupId', string), field('topics', array(topicSchema))]);

export const offsetFetchRequestV2 = defineRequest({
  apiKey: API_KEYS.OffsetFetch,
  apiVersion: 2,
  apiName: 'OffsetFetch',
  schema: requestSchema,
});
