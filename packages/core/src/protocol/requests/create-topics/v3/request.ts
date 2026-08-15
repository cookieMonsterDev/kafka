import { array, boolean, defineRequest, field, int32, object } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';
import { createTopicSchema } from '../v2/request.js';

/**
 * CreateTopics Request (Version: 3) => [create_topic_requests] timeout validate_only
 *   create_topic_requests => topic num_partitions replication_factor [replica_assignment] [config_entries]
 *     topic => STRING
 *     num_partitions => INT32
 *     replication_factor => INT16
 *     replica_assignment => partition [replicas]
 *       partition => INT32
 *       replicas => INT32
 *     config_entries => config_name config_value
 *       config_name => STRING
 *       config_value => NULLABLE_STRING
 *   timeout => INT32
 *   validate_only => BOOLEAN
 */
const requestSchema = object([
  field('topics', array(createTopicSchema)),
  field('timeout', int32),
  field('validateOnly', boolean),
]);

export const createTopicsRequestV3 = defineRequest({
  apiKey: API_KEYS.CreateTopics,
  apiVersion: 3,
  apiName: 'CreateTopics',
  schema: requestSchema,
});
