import { compactNullableString, compactString, defineRequest, field, flexibleObject, int32 } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import {
  compactTopicNames,
  nullableHeartbeatTopicPartitions,
  type ConsumerGroupHeartbeatTopicPartitions,
} from '../shared';

export interface ConsumerGroupHeartbeatRequestV1Fields {
  groupId: string;
  memberId: string;
  memberEpoch: number;
  instanceId: string | null;
  rackId: string | null;
  rebalanceTimeoutMs: number;
  subscribedTopicNames: string[] | null;
  subscribedTopicRegex: string | null;
  serverAssignor: string | null;
  topicPartitions: ConsumerGroupHeartbeatTopicPartitions[] | null;
}

/**
 * ConsumerGroupHeartbeat Request (Version: 1) => group_id member_id member_epoch instance_id rack_id
 *                                               rebalance_timeout_ms [subscribed_topic_names]
 *                                               subscribed_topic_regex server_assignor
 *                                               [topic_partitions] TAG_BUFFER
 *
 * Adds `subscribedTopicRegex` (KIP-848) after subscribed topic names. From v1 the consumer must
 * generate its own member id (KIP-1082).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('groupId', compactString),
  field('memberId', compactString),
  field('memberEpoch', int32),
  field('instanceId', compactNullableString),
  field('rackId', compactNullableString),
  field('rebalanceTimeoutMs', int32),
  field('subscribedTopicNames', compactTopicNames),
  field('subscribedTopicRegex', compactNullableString),
  field('serverAssignor', compactNullableString),
  field('topicPartitions', nullableHeartbeatTopicPartitions),
]);

export const consumerGroupHeartbeatRequestV1 = defineRequest({
  apiKey: API_KEYS.ConsumerGroupHeartbeat,
  apiVersion: 1,
  apiName: 'ConsumerGroupHeartbeat',
  schema: requestSchema,
});
