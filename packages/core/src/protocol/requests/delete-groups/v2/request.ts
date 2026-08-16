import { compactArray, compactString, defineRequest, field, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * DeleteGroups Request (Version: 2) => [groups_names] TAG_BUFFER
 *   groups_names => COMPACT_STRING
 *
 * First flexible version (KIP-482). Compact array of compact group name strings + TAG_BUFFER.
 * Request header v2's trailing TAG_BUFFER is written by `createRequest`, not here.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([field('groupIds', compactArray(compactString))]);

export const deleteGroupsRequestV2 = defineRequest({
  apiKey: API_KEYS.DeleteGroups,
  apiVersion: 2,
  apiName: 'DeleteGroups',
  schema: requestSchema,
});
