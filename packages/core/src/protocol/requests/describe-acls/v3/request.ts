import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v2/request';

/**
 * DescribeAcls Request (Version: 3) — same fields as v2. Version 3 adds the USER resource type
 * as an enum value, not a new request field.
 *
 * Flexible (KIP-482). Request header v2's trailing TAG_BUFFER is written by `createRequest`,
 * not here.
 */
export const describeAclsRequestV3 = defineRequest({
  apiKey: API_KEYS.DescribeAcls,
  apiVersion: 3,
  apiName: 'DescribeAcls',
  schema: requestSchema,
});
