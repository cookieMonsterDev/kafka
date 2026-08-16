import { Decoder } from '../../../decoder';
import {
  compactArray,
  compactBytes,
  compactNullableString,
  compactString,
  field,
  flexibleObject,
  int16,
  int32,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { joinGroupResponseV5, type JoinGroupResponseV5Body } from '../v5/response';

export type JoinGroupResponseV7Body = JoinGroupResponseV5Body & {
  protocolType: string | null;
  protocolName: string | null;
};

const memberSchema = flexibleObject([
  field('memberId', compactString),
  field('groupInstanceId', compactNullableString),
  field('memberMetadata', compactBytes),
]);
const bodySchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('generationId', int32),
  field('protocolType', compactNullableString),
  field('protocolName', compactNullableString),
  field('leaderId', compactString),
  field('memberId', compactString),
  field('members', compactArray(memberSchema)),
]);

/**
 * JoinGroup Response (Version: 7) => throttle_time_ms error_code generation_id protocol_type protocol_name
 *                                    leader_id member_id [members] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   generation_id => INT32
 *   protocol_type => COMPACT_NULLABLE_STRING
 *   protocol_name => COMPACT_NULLABLE_STRING
 *   leader_id => COMPACT_STRING
 *   member_id => COMPACT_STRING
 *   members => member_id group_instance_id member_metadata TAG_BUFFER
 *
 * Adds ProtocolType (KIP-559). ProtocolName becomes nullable. `groupProtocol` keeps the existing
 * client name for ProtocolName; `protocolType` / `protocolName` are extra fields.
 */
export const joinGroupResponseV7: ResponseDefinition<JoinGroupResponseV7Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return {
      throttleTime: 0,
      clientSideThrottleTime: decoded.throttleTime,
      errorCode: decoded.errorCode,
      generationId: decoded.generationId,
      protocolType: decoded.protocolType,
      protocolName: decoded.protocolName,
      groupProtocol: decoded.protocolName ?? '',
      leaderId: decoded.leaderId,
      memberId: decoded.memberId,
      members: decoded.members,
    };
  },
  parse: async (data) => {
    await joinGroupResponseV5.parse(data);
    return data;
  },
};
