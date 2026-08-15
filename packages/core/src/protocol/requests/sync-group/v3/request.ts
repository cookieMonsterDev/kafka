import { array, bytes, defineRequest, field, int32, nullableString, object, string } from '../../../schema.js'
import { API_KEYS } from '../../api-keys.js'

/**
 * Version 3 adds group_instance_id to indicate member identity across restarts.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances
 *
 * SyncGroup Request (Version: 3) => group_id generation_id member_id group_instance_id [group_assignment]
 *   group_id => STRING
 *   generation_id => INT32
 *   member_id => STRING
 *   group_instance_id => NULLABLE_STRING
 *   group_assignment => member_id member_assignment
 *     member_id => STRING
 *     member_assignment => BYTES
 */
const groupAssignmentSchema = object([field('memberId', string), field('memberAssignment', bytes)])
const requestSchema = object([
  field('groupId', string),
  field('generationId', int32),
  field('memberId', string),
  field('groupInstanceId', nullableString),
  field('groupAssignment', array(groupAssignmentSchema)),
])

export const syncGroupRequestV3 = defineRequest({
  apiKey: API_KEYS.SyncGroup,
  apiVersion: 3,
  apiName: 'SyncGroup',
  schema: requestSchema,
})
