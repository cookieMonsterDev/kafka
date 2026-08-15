import { array, defineRequest, field, int8, int32, int64, object, string } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';

/**
 * ListOffsets Request (Version: 2) => replica_id isolation_level [topics]
 *   replica_id => INT32
 *   isolation_level => INT8
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition timestamp
 *       partition => INT32
 *       timestamp => INT64
 */
const partitionSchema = object([field('partition', int32), field('timestamp', int64)]);
const topicSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const requestSchema = object([
  field('replicaId', int32),
  field('isolationLevel', int8),
  field('topics', array(topicSchema)),
]);

export const listOffsetsRequestV2 = defineRequest({
  apiKey: API_KEYS.ListOffsets,
  apiVersion: 2,
  apiName: 'ListOffsets',
  schema: requestSchema,
});
