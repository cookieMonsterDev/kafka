import { array, boolean, defineRequest, field, int32, object } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { topicPartitionSchema } from '../v0/request';

/**
 * CreatePartitions Request (Version: 1) => [topic_partitions] timeout validate_only
 *   topic_partitions => topic new_partitions
 *     topic => STRING
 *     new_partitions => count [assignment]
 *       count => INT32
 *       assignment => ARRAY(INT32)
 *   timeout => INT32
 *   validate_only => BOOLEAN
 */
const requestSchema = object([
  field('topicPartitions', array(topicPartitionSchema)),
  field('timeout', int32),
  field('validateOnly', boolean),
]);

export const createPartitionsRequestV1 = defineRequest({
  apiKey: API_KEYS.CreatePartitions,
  apiVersion: 1,
  apiName: 'CreatePartitions',
  schema: requestSchema,
});
