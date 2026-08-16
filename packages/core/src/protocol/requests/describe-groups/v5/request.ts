import { boolean, compactArray, compactString, defineRequest, field, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * DescribeGroups Request (Version: 5) => [groups] include_authorized_operations TAG_BUFFER
 *   groups => COMPACT_STRING
 *   include_authorized_operations => BOOLEAN
 *
 * First flexible version (KIP-482). Same fields as v3/v4. Request header v2's trailing
 * TAG_BUFFER is written by `createRequest`, not here.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('groupIds', compactArray(compactString)),
  field('includeAuthorizedOperations', boolean),
]);

export const describeGroupsRequestV5 = defineRequest({
  apiKey: API_KEYS.DescribeGroups,
  apiVersion: 5,
  apiName: 'DescribeGroups',
  schema: requestSchema,
});
