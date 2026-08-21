import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v0/request';

/**
 * ExpireDelegationToken Request (Version: 1) — same fields as version 0.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const expireDelegationTokenRequestV1 = defineRequest({
  apiKey: API_KEYS.ExpireDelegationToken,
  apiVersion: 1,
  apiName: 'ExpireDelegationToken',
  schema: requestSchema,
});
