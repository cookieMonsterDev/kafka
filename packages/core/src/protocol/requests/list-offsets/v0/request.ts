import { array, defineRequest, field, int32, int64, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * ListOffsets Request (Version: 0) => replica_id [topics]
 *   replica_id => INT32
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition timestamp max_num_offsets
 *       partition => INT32
 *       timestamp => INT64
 *       max_num_offsets => INT32
 */
const partitionSchema = object([
  field('partition', int32),
  field('timestamp', int64),
  field('maxNumOffsets', int32),
]);
const topicSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const requestSchema = object([field('replicaId', int32), field('topics', array(topicSchema))]);

export const listOffsetsRequestV0 = defineRequest({
  apiKey: API_KEYS.ListOffsets,
  apiVersion: 0,
  apiName: 'ListOffsets',
  schema: requestSchema,
});
