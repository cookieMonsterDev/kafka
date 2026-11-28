import { compactArray, compactString, defineRequest, field, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * ListGroups Request (Version: 5) => [states_filter] [types_filter] TAG_BUFFER
 *   states_filter => COMPACT_STRING
 *   types_filter => COMPACT_STRING
 *
 * Adds optional `typesFilter` (KIP-848). Empty means groups of every type. Request header v2's
 * trailing TAG_BUFFER is written by `createRequest`, not here.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('states', compactArray(compactString)),
  field('types', compactArray(compactString)),
]);

export const listGroupsRequestV5 = defineRequest({
  apiKey: API_KEYS.ListGroups,
  apiVersion: 5,
  apiName: 'ListGroups',
  schema: requestSchema,
});
