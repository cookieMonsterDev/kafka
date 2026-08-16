import { boolean, defineRequest, field, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface DescribeClusterRequestV0Fields {
  includeClusterAuthorizedOperations: boolean;
}

/**
 * DescribeCluster Request (Version: 0) => include_cluster_authorized_operations TAG_BUFFER
 *   include_cluster_authorized_operations => BOOLEAN
 *
 * Flexible from v0. Request header v2's trailing TAG_BUFFER is written by `createRequest`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([field('includeClusterAuthorizedOperations', boolean)]);

export const describeClusterRequestV0 = defineRequest({
  apiKey: API_KEYS.DescribeCluster,
  apiVersion: 0,
  apiName: 'DescribeCluster',
  schema: requestSchema,
});
