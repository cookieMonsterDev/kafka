import { array, defineRequest, field, int32, object } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { createTopicSchema } from '../v2/request';

export interface CreateTopicsRequestV0Fields {
  topics: {
    topic: string;
    numPartitions: number;
    replicationFactor: number;
    replicaAssignment: { partition: number; replicas: number[] }[];
    configEntries: { name: string; value: string | null }[];
  }[];
  timeout: number;
}

/**
 * CreateTopics Request (Version: 0) => [create_topic_requests] timeout
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
 */
const requestSchema = object([field('topics', array(createTopicSchema)), field('timeout', int32)]);

export const createTopicsRequestV0 = defineRequest({
  apiKey: API_KEYS.CreateTopics,
  apiVersion: 0,
  apiName: 'CreateTopics',
  schema: requestSchema,
});
