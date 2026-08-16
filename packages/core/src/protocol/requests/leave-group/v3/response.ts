import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes';
import { Decoder } from '../../../decoder';
import { array, field, int16, nullableString, object, string } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface LeaveGroupResponseV3Member {
  memberId: string;
  groupInstanceId: string | null;
  errorCode: number;
}

export interface LeaveGroupResponseV3Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  errorCode: number;
  members: LeaveGroupResponseV3Member[];
}

const memberSchema = object([
  field('memberId', string),
  field('groupInstanceId', nullableString),
  field('errorCode', int16),
]);
const membersSchema = array(memberSchema);

/**
 * LeaveGroup Response (Version: 3) => throttle_time_ms error_code [members]
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   members => member_id group_instance_id error_code
 *     member_id => STRING
 *     group_instance_id => NULLABLE_STRING
 *     error_code => INT16
 */
export const leaveGroupResponseV3: ResponseDefinition<LeaveGroupResponseV3Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const throttleTime = decoder.readInt32();
    const errorCode = decoder.readInt16();
    const members = membersSchema.read(decoder);

    return { throttleTime: 0, clientSideThrottleTime: throttleTime, errorCode, members };
  },
  parse: async (data) => {
    failIfVersionNotSupported(data.errorCode);
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);

    const memberWithError = data.members.find((member) => failure(member.errorCode));
    if (memberWithError) throw createErrorFromCode(memberWithError.errorCode);

    return data;
  },
};
