import { array, boolean, defineRequest, field, int32, nullableArray, object, string } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';

export interface CreatePartitionsTopicInput {
  topic: string;
  count: number;
  assignments?: number[][];
}

export interface CreatePartitionsTopicWireFields {
  topic: string;
  count: number;
  assignments: number[][];
}

export interface CreatePartitionsRequestV0Fields {
  topicPartitions: CreatePartitionsTopicWireFields[];
  timeout: number;
  validateOnly: boolean;
}

/**
 * CreatePartitions Request (Version: 0) => [topic_partitions] timeout validate_only
 *   topic_partitions => topic new_partitions
 *     topic => STRING
 *     new_partitions => count [assignment]
 *       count => INT32
 *       assignment => ARRAY(INT32)
 *   timeout => INT32
 *   validate_only => BOOLEAN
 */
export const topicPartitionSchema = object([
  field('topic', string),
  field('count', int32),
  field('assignments', nullableArray(nullableArray(int32))),
]);
const requestSchema = object([
  field('topicPartitions', array(topicPartitionSchema)),
  field('timeout', int32),
  field('validateOnly', boolean),
]);

export const createPartitionsRequestV0 = defineRequest({
  apiKey: API_KEYS.CreatePartitions,
  apiVersion: 0,
  apiName: 'CreatePartitions',
  schema: requestSchema,
});

export function withAssignmentDefaults(
  topicPartitions: readonly CreatePartitionsTopicInput[],
): CreatePartitionsTopicWireFields[] {
  return topicPartitions.map(({ topic, count, assignments = [] }) => ({ topic, count, assignments }));
}
