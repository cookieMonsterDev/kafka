import { defineRequest, field, object, string } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';

/**
 * LeaveGroup Request (Version: 0) => group_id member_id
 *   group_id => STRING
 *   member_id => STRING
 */
const requestSchema = object([field('groupId', string), field('memberId', string)]);

export const leaveGroupRequestV0 = defineRequest({
  apiKey: API_KEYS.LeaveGroup,
  apiVersion: 0,
  apiName: 'LeaveGroup',
  schema: requestSchema,
});
