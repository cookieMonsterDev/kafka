import { defineRequest, field, object, string } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';

const requestSchema = object([field('groupId', string), field('memberId', string)]);

export const leaveGroupRequestV1 = defineRequest({
  apiKey: API_KEYS.LeaveGroup,
  apiVersion: 1,
  apiName: 'LeaveGroup',
  schema: requestSchema,
});
