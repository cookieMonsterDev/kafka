import { array, defineRequest, field, int32, nullPreservingArray, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface ElectLeadersTopicPartitions {
  topic: string;
  partitions: number[];
}

export interface ElectLeadersRequestV0Fields {
  timeout: number;
  topicPartitions: ElectLeadersTopicPartitions[] | null;
}

/**
 * ElectLeaders Request (Version: 0) => [topic_partitions] timeout_ms
 *   topic_partitions => topic [partition_id]
 *     topic => STRING
 *     partition_id => INT32
 *   timeout_ms => INT32
 *
 * `topic_partitions` is nullable; null means elect preferred leaders for every partition, while
 * an empty array elects none. Originally named ElectPreferredLeaders.
 * Wire order matches the Java client: TopicPartitions, then TimeoutMs.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicSchema = object([field('topic', string), field('partitions', array(int32))]);
export const requestSchema = object([
  field('topicPartitions', nullPreservingArray(topicSchema)),
  field('timeout', int32),
]);

export const electLeadersRequestV0 = defineRequest({
  apiKey: API_KEYS.ElectLeaders,
  apiVersion: 0,
  apiName: 'ElectLeaders',
  schema: requestSchema,
});
