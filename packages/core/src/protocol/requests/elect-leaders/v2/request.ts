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
 * ElectLeaders Request (Version: 2) => election_type timeout_ms [topic_partitions] TAG_BUFFER
 *   election_type => INT8
 *   timeout_ms => INT32
 *   topic_partitions => topic [partition_id] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partition_id => INT32
 *
 * Flexible-version API. Request header v2's trailing TAG_BUFFER is written by `createRequest`.
 * `topicPartitions: null` elects preferred/unclean leaders for every partition.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(int32))]);
export const requestSchema = flexibleObject([
  field('electionType', int8),
  field('timeout', int32),
  field('topicPartitions', compactNullableArray(topicSchema)),
]);

export const electLeadersRequestV2 = defineRequest({
  apiKey: API_KEYS.ElectLeaders,
  apiVersion: 2,
  apiName: 'ElectLeaders',
  schema: requestSchema,
});
