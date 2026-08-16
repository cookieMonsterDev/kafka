import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v6/request';

export { requestSchema };

/**
 * JoinGroup Request (Version: 7) — same compact body as v6.
 */
export const joinGroupRequestV7 = defineRequest({
  apiKey: API_KEYS.JoinGroup,
  apiVersion: 7,
  apiName: 'JoinGroup',
  schema: requestSchema,
});
