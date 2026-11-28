import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v2/request';

/**
 * DeleteGroups Request (Version: 3) => [groups_names] TAG_BUFFER
 *   groups_names => COMPACT_STRING
 *
 * Same fields as v2. Asking for v3 lets the broker attach an error message to each failed group
 * in the response.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const deleteGroupsRequestV3 = defineRequest({
  apiKey: API_KEYS.DeleteGroups,
  apiVersion: 3,
  apiName: 'DeleteGroups',
  schema: requestSchema,
});
