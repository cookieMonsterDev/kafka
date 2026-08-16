import { boolean, compactString, defineRequest, field, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { compactNullableTopics } from '../v6/request';

/**
 * OffsetFetch Request (Version: 7) => group_id [topics] require_stable TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   topics => topic [partitions] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partitions => partition
 *       partition => INT32
 *   require_stable => BOOLEAN
 *
 * Flexible v6 plus `require_stable` (KIP-664). Empty `topics` is still compact-null ("all topics").
 * Request header v2's trailing TAG_BUFFER is written by `createRequest`, not here.
 */
export const requestSchema = flexibleObject([
  field('groupId', compactString),
  field('topics', compactNullableTopics),
  field('requireStable', boolean),
]);

export const offsetFetchRequestV7 = defineRequest({
  apiKey: API_KEYS.OffsetFetch,
  apiVersion: 7,
  apiName: 'OffsetFetch',
  schema: requestSchema,
});
