import { boolean, defineRequest, field, flexibleObject, int8 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface DescribeClusterRequestV1Fields {
  includeClusterAuthorizedOperations: boolean;
  endpointType: number;
}

/**
 * DescribeCluster Request (Version: 1) => include_cluster_authorized_operations endpoint_type TAG_BUFFER
 *   include_cluster_authorized_operations => BOOLEAN
 *   endpoint_type => INT8
 *
 * Adds `endpointType` (KIP-919): 1=brokers, 2=controllers.
 * Flexible-version API. Request header v2's trailing TAG_BUFFER is written by `createRequest`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('includeClusterAuthorizedOperations', boolean),
  field('endpointType', int8),
]);

export const describeClusterRequestV1 = defineRequest({
  apiKey: API_KEYS.DescribeCluster,
  apiVersion: 1,
  apiName: 'DescribeCluster',
  schema: requestSchema,
});
