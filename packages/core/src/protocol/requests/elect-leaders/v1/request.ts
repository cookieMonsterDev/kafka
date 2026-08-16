import { array, defineRequest, field, int32, int8, nullableArray, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import type { ElectLeadersTopicPartitions } from '../v0/request';

export type { ElectLeadersTopicPartitions };

export interface ElectLeadersRequestV1Fields {
  electionType: number;
  timeout: number;
  topicPartitions: ElectLeadersTopicPartitions[];
}

/**
 * ElectLeaders Request (Version: 1) => election_type [topic_partitions] timeout_ms
 *   election_type => INT8
 *   topic_partitions => topic [partition_id]
 *     topic => STRING
 *     partition_id => INT32
 *   timeout_ms => INT32
 *
 * `election_type`: 0 preferred, 1 unclean. Wire order matches the Java client:
 * ElectionType, TopicPartitions, TimeoutMs (KIP-460).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicSchema = object([field('topic', string), field('partitions', array(int32))]);
export const requestSchema = object([
  field('electionType', int8),
  field('topicPartitions', nullableArray(topicSchema)),
  field('timeout', int32),
]);

export const electLeadersRequestV1 = defineRequest({
  apiKey: API_KEYS.ElectLeaders,
  apiVersion: 1,
  apiName: 'ElectLeaders',
  schema: requestSchema,
});
