import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v5/request';

/**
 * DescribeGroups Request (Version: 6) => [groups] include_authorized_operations TAG_BUFFER
 *   groups => COMPACT_STRING
 *   include_authorized_operations => BOOLEAN
 *
 * Same fields as v5. Asking for v6 makes the broker answer an unknown group id with
 * GROUP_ID_NOT_FOUND instead of an empty `Dead` group (KIP-1043).
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const describeGroupsRequestV6 = defineRequest({
  apiKey: API_KEYS.DescribeGroups,
  apiVersion: 6,
  apiName: 'DescribeGroups',
  schema: requestSchema,
});
