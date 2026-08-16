import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v8/request';

export { requestSchema };

/**
 * JoinGroup Request (Version: 9) — same compact body as v8.
 */
export const joinGroupRequestV9 = defineRequest({
  apiKey: API_KEYS.JoinGroup,
  apiVersion: 9,
  apiName: 'JoinGroup',
  schema: requestSchema,
});
