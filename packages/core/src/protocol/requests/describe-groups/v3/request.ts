import { array, boolean, defineRequest, field, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * DescribeGroups Request (Version: 3) => [groups] include_authorized_operations
 *   groups => STRING
 *   include_authorized_operations => BOOLEAN
 *
 * Adds `includeAuthorizedOperations` (KIP-430). Same wire as v4.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = object([field('groupIds', array(string)), field('includeAuthorizedOperations', boolean)]);

export function createDescribeGroupsRequestWithAuthorizedOps(apiVersion: 3 | 4) {
  return defineRequest({
    apiKey: API_KEYS.DescribeGroups,
    apiVersion,
    apiName: 'DescribeGroups',
    schema: requestSchema,
  });
}

export const describeGroupsRequestV3 = createDescribeGroupsRequestWithAuthorizedOps(3);
