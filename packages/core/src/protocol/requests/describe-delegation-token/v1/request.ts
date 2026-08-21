import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v0/request';

/**
 * DescribeDelegationToken Request (Version: 1) — same fields as version 0.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const describeDelegationTokenRequestV1 = defineRequest({
  apiKey: API_KEYS.DescribeDelegationToken,
  apiVersion: 1,
  apiName: 'DescribeDelegationToken',
  schema: requestSchema,
});
