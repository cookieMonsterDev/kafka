import { array, defineRequest, field, int32, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * OffsetFetch Request (Version: 1) => group_id [topics]
 *   group_id => STRING
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition
 *       partition => INT32
 */
const partitionSchema = object([field('partition', int32)]);
const topicSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const requestSchema = object([field('groupId', string), field('topics', array(topicSchema))]);

export const offsetFetchRequestV1 = defineRequest({
  apiKey: API_KEYS.OffsetFetch,
  apiVersion: 1,
  apiName: 'OffsetFetch',
  schema: requestSchema,
});
