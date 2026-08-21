import { compactNullableString, compactString, defineRequest, field, flexibleObject, int32 } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import {
  compactTopicNames,
  nullableHeartbeatTopicPartitions,
  type ConsumerGroupHeartbeatTopicPartitions,
} from '../shared';

export interface ConsumerGroupHeartbeatRequestV0Fields {
  groupId: string;
  memberId: string;
  memberEpoch: number;
  instanceId: string | null;
  rackId: string | null;
  rebalanceTimeoutMs: number;
  subscribedTopicNames: string[] | null;
  serverAssignor: string | null;
  topicPartitions: ConsumerGroupHeartbeatTopicPartitions[] | null;
}

/**
 * ConsumerGroupHeartbeat Request (Version: 0) => group_id member_id member_epoch instance_id rack_id
 *                                               rebalance_timeout_ms [subscribed_topic_names]
 *                                               server_assignor [topic_partitions] TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   member_id => COMPACT_STRING
 *   member_epoch => INT32
 *   instance_id => COMPACT_NULLABLE_STRING
 *   rack_id => COMPACT_NULLABLE_STRING
 *   rebalance_timeout_ms => INT32
 *   subscribed_topic_names => COMPACT_NULLABLE_ARRAY of COMPACT_STRING
 *   server_assignor => COMPACT_NULLABLE_STRING
 *   topic_partitions => topic_id [partitions] TAG_BUFFER
 *     topic_id => UUID
 *     partitions => INT32
 *
 * Flexible from v0 (KIP-848). Conditional fields are null / -1 when unchanged since the last
 * heartbeat. Member epoch 0 joins, -1 leaves, -2 is a static-member rejoin.
 * Request header v2's trailing TAG_BUFFER is written by `createRequest`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
 */
export const requestSchema = flexibleObject([
  field('groupId', compactString),
  field('memberId', compactString),
  field('memberEpoch', int32),
  field('instanceId', compactNullableString),
  field('rackId', compactNullableString),
  field('rebalanceTimeoutMs', int32),
  field('subscribedTopicNames', compactTopicNames),
  field('serverAssignor', compactNullableString),
  field('topicPartitions', nullableHeartbeatTopicPartitions),
]);

export const consumerGroupHeartbeatRequestV0 = defineRequest({
  apiKey: API_KEYS.ConsumerGroupHeartbeat,
  apiVersion: 0,
  apiName: 'ConsumerGroupHeartbeat',
  schema: requestSchema,
});
