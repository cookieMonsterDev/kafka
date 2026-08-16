import {
  compactArray,
  compactNullableArray,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int32,
  int8,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';
import type { ElectLeadersTopicPartitions } from '../v0/request';

export type { ElectLeadersTopicPartitions };

export interface ElectLeadersRequestV2Fields {
  electionType: number;
  timeout: number;
  topicPartitions: ElectLeadersTopicPartitions[] | null;
}

/**
 * ElectLeaders Request (Version: 2) => election_type [topic_partitions] timeout_ms TAG_BUFFER
 *   election_type => INT8
 *   topic_partitions => topic [partition_id] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partition_id => INT32
 *   timeout_ms => INT32
 *
 * Flexible-version API. Request header v2's trailing TAG_BUFFER is written by `createRequest`.
 * `topicPartitions: null` elects preferred/unclean leaders for every partition.
 * Wire order matches the Java client: ElectionType, TopicPartitions, TimeoutMs.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(int32))]);
export const requestSchema = flexibleObject([
  field('electionType', int8),
  field('topicPartitions', compactNullableArray(topicSchema)),
  field('timeout', int32),
]);

export const electLeadersRequestV2 = defineRequest({
  apiKey: API_KEYS.ElectLeaders,
  apiVersion: 2,
  apiName: 'ElectLeaders',
  schema: requestSchema,
});
