import { array, defineRequest, field, int32, nullableArray, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface ElectLeadersTopicPartitions {
  topic: string;
  partitions: number[];
}

export interface ElectLeadersRequestV0Fields {
  timeout: number;
  topicPartitions: ElectLeadersTopicPartitions[];
}

/**
 * ElectLeaders Request (Version: 0) => timeout_ms [topic_partitions]
 *   timeout_ms => INT32
 *   topic_partitions => topic [partition_id]
 *     topic => STRING
 *     partition_id => INT32
 *
 * `topic_partitions` is nullable; null (encoded as empty via `nullableArray`) means elect
 * preferred leaders for every partition. Originally named ElectPreferredLeaders.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicSchema = object([field('topic', string), field('partitions', array(int32))]);
export const requestSchema = object([field('timeout', int32), field('topicPartitions', nullableArray(topicSchema))]);

export const electLeadersRequestV0 = defineRequest({
  apiKey: API_KEYS.ElectLeaders,
  apiVersion: 0,
  apiName: 'ElectLeaders',
  schema: requestSchema,
});
