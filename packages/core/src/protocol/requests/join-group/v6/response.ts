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

export type JoinGroupResponseV6Body = JoinGroupResponseV5Body;

const memberSchema = flexibleObject([
  field('memberId', compactString),
  field('groupInstanceId', compactNullableString),
  field('memberMetadata', compactBytes),
]);
const bodySchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('generationId', int32),
  field('groupProtocol', compactString),
  field('leaderId', compactString),
  field('memberId', compactString),
  field('members', compactArray(memberSchema)),
]);

/**
 * JoinGroup Response (Version: 6) => throttle_time_ms error_code generation_id group_protocol leader_id
 *                                    member_id [members] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   generation_id => INT32
 *   group_protocol => COMPACT_STRING
 *   leader_id => COMPACT_STRING
 *   member_id => COMPACT_STRING
 *   members => member_id group_instance_id member_metadata TAG_BUFFER
 *     member_id => COMPACT_STRING
 *     group_instance_id => COMPACT_NULLABLE_STRING
 *     member_metadata => COMPACT_BYTES
 *
 * First flexible version (KIP-482). Same fields as v5. Quota timing follows v3 (KIP-219).
 * Response header v1's trailing TAG_BUFFER is skipped by `Connection` before `decode()` runs.
 */
export const joinGroupResponseV6: ResponseDefinition<JoinGroupResponseV6Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await joinGroupResponseV5.parse(data);
    return data;
  },
};
