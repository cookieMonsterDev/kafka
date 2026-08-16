import { array, bytes, defineRequest, field, int32, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * SyncGroup Request (Version: 0) => group_id generation_id member_id [group_assignment]
 *   group_id => STRING
 *   generation_id => INT32
 *   member_id => STRING
 *   group_assignment => member_id member_assignment
 *     member_id => STRING
 *     member_assignment => BYTES
 *
 * `memberAssignment` must be a `Buffer` (typically from `AssignerProtocol.MemberAssignment.encode`).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const groupAssignmentSchema = object([field('memberId', string), field('memberAssignment', bytes)]);
const requestSchema = object([
  field('groupId', string),
  field('generationId', int32),
  field('memberId', string),
  field('groupAssignment', array(groupAssignmentSchema)),
]);

export const syncGroupRequestV0 = defineRequest({
  apiKey: API_KEYS.SyncGroup,
  apiVersion: 0,
  apiName: 'SyncGroup',
  schema: requestSchema,
});
