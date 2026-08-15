import { array, bytes, defineRequest, field, int32, object, string } from '../../../schema.js'
import { API_KEYS } from '../../api-keys.js'

/**
 * SyncGroup Request (Version: 0) => group_id generation_id member_id [group_assignment]
 *   group_id => STRING
 *   generation_id => INT32
 *   member_id => STRING
 *   group_assignment => member_id member_assignment
 *     member_id => STRING
 *     member_assignment => BYTES
 *
 * kafkajs's `writeBytes` falls back to `String(value)` for a non-Buffer, non-string
 * `memberAssignment` — its own v0 request unit test relies on this and captures the literal
 * string `"[object Object]"` in its fixture. `bytes` here requires a real `Buffer`, so that
 * particular footgun isn't reproducible; real callers always pass an encoded assignment anyway
 * (e.g. `AssignerProtocol.MemberAssignment.encode(...)`).
 */
const groupAssignmentSchema = object([field('memberId', string), field('memberAssignment', bytes)])
const requestSchema = object([
  field('groupId', string),
  field('generationId', int32),
  field('memberId', string),
  field('groupAssignment', array(groupAssignmentSchema)),
])

export const syncGroupRequestV0 = defineRequest({
  apiKey: API_KEYS.SyncGroup,
  apiVersion: 0,
  apiName: 'SyncGroup',
  schema: requestSchema,
})
