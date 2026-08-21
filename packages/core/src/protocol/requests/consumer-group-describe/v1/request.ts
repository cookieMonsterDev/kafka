import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema, type ConsumerGroupDescribeRequestV0Fields } from '../v0/request';

export type ConsumerGroupDescribeRequestV1Fields = ConsumerGroupDescribeRequestV0Fields;

/**
 * ConsumerGroupDescribe Request (Version: 1) is the same as version 0. Version 1 adds
 * `memberType` on the response (KIP-1099).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export { requestSchema };

export const consumerGroupDescribeRequestV1 = defineRequest({
  apiKey: API_KEYS.ConsumerGroupDescribe,
  apiVersion: 1,
  apiName: 'ConsumerGroupDescribe',
  schema: requestSchema,
});
