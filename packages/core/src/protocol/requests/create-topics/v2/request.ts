import { array, boolean, defineRequest, field, int16, int32, nullableString, object, string } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';

export interface CreateTopicReplicaAssignment {
  partition: number;
  replicas: number[];
}

export interface CreateTopicConfigEntry {
  name: string;
  value: string | null;
}

export interface CreateTopicInput {
  topic: string;
  numPartitions?: number;
  replicationFactor?: number;
  replicaAssignment?: CreateTopicReplicaAssignment[];
  configEntries?: CreateTopicConfigEntry[];
}

export interface CreateTopicWireFields {
  topic: string;
  numPartitions: number;
  replicationFactor: number;
  replicaAssignment: CreateTopicReplicaAssignment[];
  configEntries: CreateTopicConfigEntry[];
}

export interface CreateTopicsRequestV2Fields {
  topics: CreateTopicWireFields[];
  timeout: number;
  validateOnly: boolean;
}

/**
 * CreateTopics Request (Version: 2) => [create_topic_requests] timeout validate_only
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
export const replicaAssignmentSchema = object([field('partition', int32), field('replicas', array(int32))]);
export const configEntrySchema = object([field('name', string), field('value', nullableString)]);
export const createTopicSchema = object([
  field('topic', string),
  field('numPartitions', int32),
  field('replicationFactor', int16),
  field('replicaAssignment', array(replicaAssignmentSchema)),
  field('configEntries', array(configEntrySchema)),
]);
const requestSchema = object([
  field('topics', array(createTopicSchema)),
  field('timeout', int32),
  field('validateOnly', boolean),
]);

export const createTopicsRequestV2 = defineRequest({
  apiKey: API_KEYS.CreateTopics,
  apiVersion: 2,
  apiName: 'CreateTopics',
  schema: requestSchema,
});

export function withTopicDefaults(topics: readonly CreateTopicInput[]): CreateTopicWireFields[] {
  return topics.map(
    ({ topic, numPartitions = -1, replicationFactor = -1, replicaAssignment = [], configEntries = [] }) => ({
      topic,
      numPartitions,
      replicationFactor,
      replicaAssignment,
      configEntries,
    }),
  );
}
