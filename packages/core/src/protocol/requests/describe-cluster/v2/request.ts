import { boolean, defineRequest, field, flexibleObject, int8 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface DescribeClusterRequestV2Fields {
  includeClusterAuthorizedOperations: boolean;
  endpointType: number;
  includeFencedBrokers: boolean;
}

/**
 * DescribeCluster Request (Version: 2) => include_cluster_authorized_operations endpoint_type
 *                                         include_fenced_brokers TAG_BUFFER
 *   include_cluster_authorized_operations => BOOLEAN
 *   endpoint_type => INT8
 *   include_fenced_brokers => BOOLEAN
 *
 * Adds `includeFencedBrokers` (KIP-1073). Flexible-version API.
 * Request header v2's trailing TAG_BUFFER is written by `createRequest`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('includeClusterAuthorizedOperations', boolean),
  field('endpointType', int8),
  field('includeFencedBrokers', boolean),
]);

export const describeClusterRequestV2 = defineRequest({
  apiKey: API_KEYS.DescribeCluster,
  apiVersion: 2,
  apiName: 'DescribeCluster',
  schema: requestSchema,
});
