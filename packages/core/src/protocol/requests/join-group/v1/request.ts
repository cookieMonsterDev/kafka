import { array, bytes, defineRequest, field, int32, object, string } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';

/**
 * JoinGroup Request (Version: 1) => group_id session_timeout rebalance_timeout member_id protocol_type [group_protocols]
 *   group_id => STRING
 *   session_timeout => INT32
 *   rebalance_timeout => INT32
 *   member_id => STRING
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
  field('protocolType', string),
  field('groupProtocols', array(groupProtocolSchema)),
]);

export const joinGroupRequestV1 = defineRequest({
  apiKey: API_KEYS.JoinGroup,
  apiVersion: 1,
  apiName: 'JoinGroup',
  schema: requestSchema,
});
