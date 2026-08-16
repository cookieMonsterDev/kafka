import {
  compactArray,
  compactBytes,
  compactNullableString,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int32,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * JoinGroup Request (Version: 8) => group_id session_timeout rebalance_timeout member_id group_instance_id
 *                                   protocol_type [group_protocols] reason TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   session_timeout => INT32
 *   rebalance_timeout => INT32
 *   member_id => COMPACT_STRING
 *   group_instance_id => COMPACT_NULLABLE_STRING
 *   protocol_type => COMPACT_STRING
 *   group_protocols => protocol_name protocol_metadata TAG_BUFFER
 *     protocol_name => COMPACT_STRING
 *     protocol_metadata => COMPACT_BYTES
 *   reason => COMPACT_NULLABLE_STRING
 *
 * Adds Reason (KIP-800).
 */
const groupProtocolSchema = flexibleObject([field('name', compactString), field('metadata', compactBytes)]);
export const requestSchema = flexibleObject([
  field('groupId', compactString),
  field('sessionTimeout', int32),
  field('rebalanceTimeout', int32),
  field('memberId', compactString),
  field('groupInstanceId', compactNullableString),
  field('protocolType', compactString),
  field('groupProtocols', compactArray(groupProtocolSchema)),
  field('reason', compactNullableString),
]);

export const joinGroupRequestV8 = defineRequest({
  apiKey: API_KEYS.JoinGroup,
  apiVersion: 8,
  apiName: 'JoinGroup',
  schema: requestSchema,
});
