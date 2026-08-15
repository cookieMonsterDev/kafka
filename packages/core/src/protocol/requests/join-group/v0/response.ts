import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes.js'
import { array, bytes, defineResponse, field, int16, int32, object, string } from '../../../schema.js'

/**
 * JoinGroup Response (Version: 0) => error_code generation_id group_protocol leader_id member_id [members]
 *   error_code => INT16
 *   generation_id => INT32
 *   group_protocol => STRING
 *   leader_id => STRING
 *   member_id => STRING
 *   members => member_id member_metadata
 *     member_id => STRING
 *     member_metadata => BYTES
 */
export const memberSchema = object([field('memberId', string), field('memberMetadata', bytes)])
const bodySchema = object([
  field('errorCode', int16),
  field('generationId', int32),
  field('groupProtocol', string),
  field('leaderId', string),
  field('memberId', string),
  field('members', array(memberSchema)),
])

export const joinGroupResponseV0 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    failIfVersionNotSupported(data.errorCode)
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode)
    return data
  },
})
