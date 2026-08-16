import { defineRequest, field, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

const requestSchema = object([field('groupId', string), field('memberId', string)]);

export const leaveGroupRequestV1 = defineRequest({
  apiKey: API_KEYS.LeaveGroup,
  apiVersion: 1,
  apiName: 'LeaveGroup',
  schema: requestSchema,
});
