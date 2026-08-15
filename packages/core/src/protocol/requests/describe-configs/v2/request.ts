import { defineRequest } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';
import { requestSchema } from '../v1/request.js';

export { type DescribeConfigsRequestV1Fields as DescribeConfigsRequestV2Fields } from '../v1/request.js';
export { type DescribeConfigsResource, withDefaultConfigNames } from '../v1/request.js';

/**
 * DescribeConfigs Request (Version: 2) — wire format identical to v1; only the response's
 * throttling semantics change (KIP-219).
 */
export const describeConfigsRequestV2 = defineRequest({
  apiKey: API_KEYS.DescribeConfigs,
  apiVersion: 2,
  apiName: 'DescribeConfigs',
  schema: requestSchema,
});
