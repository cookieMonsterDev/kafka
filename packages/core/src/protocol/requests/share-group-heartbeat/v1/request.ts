import { compactNullableString, compactString, defineRequest, field, flexibleObject, int32 } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { compactTopicNames } from '../shared';

export interface ShareGroupHeartbeatRequestV1Fields {
  groupId: string;
  memberId: string;
  memberEpoch: number;
  rackId: string | null;
  subscribedTopicNames: string[] | null;
}

/**
 * ShareGroupHeartbeat Request (Version: 1) => group_id member_id member_epoch rack_id
 *                                               [subscribed_topic_names] TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   member_id => COMPACT_STRING
 *   member_epoch => INT32
 *   rack_id => COMPACT_NULLABLE_STRING
 *   subscribed_topic_names => COMPACT_NULLABLE_ARRAY of COMPACT_STRING
 *
 * Flexible from v0 (KIP-932). Member epoch 0 joins, -1 leaves. Conditional fields are null when
 * unchanged since the last heartbeat. Request header v2's trailing TAG_BUFFER is written by
 * `createRequest`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka
 */
export const requestSchema = flexibleObject([
  field('groupId', compactString),
  field('memberId', compactString),
  field('memberEpoch', int32),
  field('rackId', compactNullableString),
  field('subscribedTopicNames', compactTopicNames),
]);

export const shareGroupHeartbeatRequestV1 = defineRequest({
  apiKey: API_KEYS.ShareGroupHeartbeat,
  apiVersion: 1,
  apiName: 'ShareGroupHeartbeat',
  schema: requestSchema,
});
