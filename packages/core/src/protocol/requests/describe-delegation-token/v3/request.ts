import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v2/request';

/**
 * DescribeDelegationToken Request (Version: 3) — same request fields as version 2.
 * Version 3 adds the token requester into the response.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const describeDelegationTokenRequestV3 = defineRequest({
  apiKey: API_KEYS.DescribeDelegationToken,
  apiVersion: 3,
  apiName: 'DescribeDelegationToken',
  schema: requestSchema,
});
