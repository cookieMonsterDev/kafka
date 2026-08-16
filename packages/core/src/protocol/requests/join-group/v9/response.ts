import { Decoder } from '../../../decoder';
import {
  boolean,
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
import { joinGroupResponseV5 } from '../v5/response';
import type { JoinGroupResponseV7Body } from '../v7/response';

export type JoinGroupResponseV9Body = JoinGroupResponseV7Body & {
  skipAssignment: boolean;
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
  field('skipAssignment', boolean),
  field('memberId', compactString),
  field('members', compactArray(memberSchema)),
]);

/**
 * JoinGroup Response (Version: 9) => throttle_time_ms error_code generation_id protocol_type protocol_name
 *                                    leader_id skip_assignment member_id [members] TAG_BUFFER
 *   skip_assignment => BOOLEAN
 *
 * Adds SkipAssignment after Leader. `groupProtocol` keeps the existing client name.
 */
export const joinGroupResponseV9: ResponseDefinition<JoinGroupResponseV9Body> = {
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
      skipAssignment: decoded.skipAssignment,
      memberId: decoded.memberId,
      members: decoded.members,
    };
  },
  parse: async (data) => {
    await joinGroupResponseV5.parse(data);
    return data;
  },
};
