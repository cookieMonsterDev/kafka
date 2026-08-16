import { array, bytes, defineRequest, field, int32, nullableString, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * Version 5 adds group_instance_id to identify members across restarts.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances
 *
 * JoinGroup Request (Version: 5) => group_id session_timeout rebalance_timeout member_id group_instance_id protocol_type [group_protocols]
 *   group_id => STRING
 *   session_timeout => INT32
 *   rebalance_timeout => INT32
 *   member_id => STRING
 *   group_instance_id => NULLABLE_STRING
 *   protocol_type => STRING
 *   group_protocols => protocol_name protocol_metadata
 *     protocol_name => STRING
 *     protocol_metadata => BYTES
 */
const groupProtocolSchema = object([field('name', string), field('metadata', bytes)]);
const requestSchema = object([
  field('groupId', string),
  field('sessionTimeout', int32),
  field('rebalanceTimeout', int32),
  field('memberId', string),
  field('groupInstanceId', nullableString),
  field('protocolType', string),
  field('groupProtocols', array(groupProtocolSchema)),
]);

export const joinGroupRequestV5 = defineRequest({
  apiKey: API_KEYS.JoinGroup,
  apiVersion: 5,
  apiName: 'JoinGroup',
  schema: requestSchema,
});
