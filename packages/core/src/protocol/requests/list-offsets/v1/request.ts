import { array, defineRequest, field, int32, int64, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * ListOffsets Request (Version: 1) => replica_id [topics]
 *   replica_id => INT32
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition timestamp
 *       partition => INT32
 *       timestamp => INT64
 */
const partitionSchema = object([field('partition', int32), field('timestamp', int64)]);
const topicSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const requestSchema = object([field('replicaId', int32), field('topics', array(topicSchema))]);

export const listOffsetsRequestV1 = defineRequest({
  apiKey: API_KEYS.ListOffsets,
  apiVersion: 1,
  apiName: 'ListOffsets',
  schema: requestSchema,
});
