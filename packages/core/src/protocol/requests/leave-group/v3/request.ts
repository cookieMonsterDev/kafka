import { array, defineRequest, field, nullableString, object, string } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';

/**
 * Version 3 changes LeaveGroup to operate on a batch of members and adds group_instance_id to
 * identify members across restarts.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances
 *
 * LeaveGroup Request (Version: 3) => group_id [members]
 *   group_id => STRING
 *   members => member_id group_instance_id
 *     member_id => STRING
 *     group_instance_id => NULLABLE_STRING
 */
const memberSchema = object([field('memberId', string), field('groupInstanceId', nullableString)]);
const requestSchema = object([field('groupId', string), field('members', array(memberSchema))]);

export const leaveGroupRequestV3 = defineRequest({
  apiKey: API_KEYS.LeaveGroup,
  apiVersion: 3,
  apiName: 'LeaveGroup',
  schema: requestSchema,
});
