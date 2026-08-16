import { Decoder } from '../../../decoder';
import {
  compactArray,
  compactNullableString,
  compactString,
  field,
  flexibleObject,
  int16,
  int32,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { leaveGroupResponseV3, type LeaveGroupResponseV3Body } from '../v3/response';

export type LeaveGroupResponseV4Body = LeaveGroupResponseV3Body;

const memberSchema = flexibleObject([
  field('memberId', compactString),
  field('groupInstanceId', compactNullableString),
  field('errorCode', int16),
]);
const bodySchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('members', compactArray(memberSchema)),
]);

/**
 * LeaveGroup Response (Version: 4) => throttle_time_ms error_code [members] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   members => member_id group_instance_id error_code TAG_BUFFER
 *     member_id => COMPACT_STRING
 *     group_instance_id => COMPACT_NULLABLE_STRING
 *     error_code => INT16
 *
 * First flexible version (KIP-482). Same fields as v3. Quota timing follows v2 (KIP-219).
 * Response header v1's trailing TAG_BUFFER is skipped by `Connection` before `decode()` runs.
 */
export const leaveGroupResponseV4: ResponseDefinition<LeaveGroupResponseV4Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await leaveGroupResponseV3.parse(data);
    return data;
  },
};
