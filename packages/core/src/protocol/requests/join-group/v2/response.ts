import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes';
import { array, bytes, defineResponse, field, int16, int32, object, string } from '../../../schema';

/**
 * JoinGroup Response (Version: 2) => throttle_time_ms error_code generation_id group_protocol leader_id member_id [members]
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   generation_id => INT32
 *   group_protocol => STRING
 *   leader_id => STRING
 *   member_id => STRING
 *   members => member_id member_metadata
 */
const memberSchema = object([field('memberId', string), field('memberMetadata', bytes)]);
const bodySchema = object([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('generationId', int32),
  field('groupProtocol', string),
  field('leaderId', string),
  field('memberId', string),
  field('members', array(memberSchema)),
]);

export const joinGroupResponseV2 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    failIfVersionNotSupported(data.errorCode);
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
});
