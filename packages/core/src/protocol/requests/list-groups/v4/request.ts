import { compactArray, compactString, defineRequest, field, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * ListGroups Request (Version: 4) => [states_filter] TAG_BUFFER
 *   states_filter => COMPACT_STRING
 *
 * Adds optional `statesFilter` (KIP-518). Empty means all groups. Request header v2's trailing
 * TAG_BUFFER is written by `createRequest`, not here.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([field('states', compactArray(compactString))]);

export const listGroupsRequestV4 = defineRequest({
  apiKey: API_KEYS.ListGroups,
  apiVersion: 4,
  apiName: 'ListGroups',
  schema: requestSchema,
});
