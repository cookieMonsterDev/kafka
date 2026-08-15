import { defineRequest, field, int32, object, string } from '../../../schema.js'
import { API_KEYS } from '../../api-keys.js'

/**
 * Heartbeat Request (Version: 0) => group_id group_generation_id member_id
 *   group_id => STRING
 *   group_generation_id => INT32
 *   member_id => STRING
 */
const requestSchema = object([
  field('groupId', string),
  field('groupGenerationId', int32),
  field('memberId', string),
])

export const heartbeatRequestV0 = defineRequest({
  apiKey: API_KEYS.Heartbeat,
  apiVersion: 0,
  apiName: 'Heartbeat',
  schema: requestSchema,
})
