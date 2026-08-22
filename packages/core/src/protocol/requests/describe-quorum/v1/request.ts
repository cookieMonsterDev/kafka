import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema, type DescribeQuorumRequestV0Fields } from '../v0/request';

export type DescribeQuorumRequestV1Fields = DescribeQuorumRequestV0Fields;

/**
 * DescribeQuorum Request (Version: 1) is the same as version 0. Version 1 adds replica
 * timestamps on the response (KIP-836).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export { requestSchema };

export const describeQuorumRequestV1 = defineRequest({
  apiKey: API_KEYS.DescribeQuorum,
  apiVersion: 1,
  apiName: 'DescribeQuorum',
  schema: requestSchema,
});
