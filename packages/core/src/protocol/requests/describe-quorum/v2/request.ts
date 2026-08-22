import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema, type DescribeQuorumRequestV0Fields } from '../v0/request';

export type DescribeQuorumRequestV2Fields = DescribeQuorumRequestV0Fields;

/**
 * DescribeQuorum Request (Version: 2) is the same as version 0. Version 2 adds error messages,
 * replica directory IDs, and node listeners on the response (KIP-853).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export { requestSchema };

export const describeQuorumRequestV2 = defineRequest({
  apiKey: API_KEYS.DescribeQuorum,
  apiVersion: 2,
  apiName: 'DescribeQuorum',
  schema: requestSchema,
});
