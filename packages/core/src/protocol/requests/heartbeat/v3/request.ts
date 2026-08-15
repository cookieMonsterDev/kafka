import { defineRequest, field, int32, nullableString, object, string } from '../../../schema.js'
import { API_KEYS } from '../../api-keys.js'

/**
 * Version 3 adds group_instance_id to indicate member identity across restarts.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances
 *
 * Heartbeat Request (Version: 3) => group_id generation_id member_id group_instance_id
 *   group_id => STRING
 *   generation_id => INT32
 *   member_id => STRING
 *   group_instance_id => NULLABLE_STRING
 */
const requestSchema = object([
  field('groupId', string),
  field('groupGenerationId', int32),
  field('memberId', string),
  field('groupInstanceId', nullableString),
])

export const heartbeatRequestV3 = defineRequest({
  apiKey: API_KEYS.Heartbeat,
  apiVersion: 3,
  apiName: 'Heartbeat',
  schema: requestSchema,
})
