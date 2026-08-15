import { array, bytes, defineRequest, field, int32, object, string } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';

export interface GroupProtocol {
  name: string;
  metadata?: Buffer;
}

export interface JoinGroupRequestV0Fields {
  groupId: string;
  sessionTimeout: number;
  memberId: string;
  protocolType: string;
  groupProtocols: GroupProtocol[];
}

/**
 * JoinGroup Request (Version: 0) => group_id session_timeout member_id protocol_type [group_protocols]
 *   group_id => STRING
 *   session_timeout => INT32
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
  field('memberId', string),
  field('protocolType', string),
  field('groupProtocols', array(groupProtocolSchema)),
]);

export const joinGroupRequestV0 = defineRequest({
  apiKey: API_KEYS.JoinGroup,
  apiVersion: 0,
  apiName: 'JoinGroup',
  schema: requestSchema,
});

export function withDefaultMetadata(groupProtocols: readonly GroupProtocol[]): { name: string; metadata: Buffer }[] {
  return groupProtocols.map(({ name, metadata = Buffer.alloc(0) }) => ({ name, metadata }));
}
