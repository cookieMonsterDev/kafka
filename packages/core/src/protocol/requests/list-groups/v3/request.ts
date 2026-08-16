import { defineRequest, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * ListGroups Request (Version: 3) => TAG_BUFFER
 *
 * First flexible version (KIP-482). Empty body plus a trailing TAG_BUFFER. Request header v2's
 * trailing TAG_BUFFER is written by `createRequest`, not here.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([]);

export const listGroupsRequestV3 = defineRequest({
  apiKey: API_KEYS.ListGroups,
  apiVersion: 3,
  apiName: 'ListGroups',
  schema: requestSchema,
});
