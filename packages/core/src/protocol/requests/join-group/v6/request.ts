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
 * JoinGroup Request (Version: 6) => group_id session_timeout rebalance_timeout member_id group_instance_id
 *                                   protocol_type [group_protocols] TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   session_timeout => INT32
 *   rebalance_timeout => INT32
 *   member_id => COMPACT_STRING
 *   group_instance_id => COMPACT_NULLABLE_STRING
 *   protocol_type => COMPACT_STRING
 *   group_protocols => protocol_name protocol_metadata TAG_BUFFER
 *     protocol_name => COMPACT_STRING
 *     protocol_metadata => COMPACT_BYTES
 *
 * First flexible version (KIP-482). Same fields as v5. Request header v2's trailing TAG_BUFFER is
 * written by `createRequest`, not here.
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
]);

export const joinGroupRequestV6 = defineRequest({
  apiKey: API_KEYS.JoinGroup,
  apiVersion: 6,
  apiName: 'JoinGroup',
  schema: requestSchema,
});
