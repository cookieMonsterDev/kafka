import { boolean, compactArray, compactString, defineRequest, field, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { compactNullableTopics } from '../v6/request';

const groupSchema = flexibleObject([field('groupId', compactString), field('topics', compactNullableTopics)]);

/**
 * OffsetFetch Request (Version: 8) => [groups] require_stable TAG_BUFFER
 *   groups => group_id [topics] TAG_BUFFER
 *     group_id => COMPACT_STRING
 *     topics => name [partition_indexes] TAG_BUFFER
 *       name => COMPACT_STRING
 *       partition_indexes => INT32
 *   require_stable => BOOLEAN
 *
 * Fetches the committed offsets of several groups in one request (KIP-709). An empty `topics`
 * list is still written as a compact null ("all topics"), per group.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('groups', compactArray(groupSchema)),
  field('requireStable', boolean),
]);

export const offsetFetchRequestV8 = defineRequest({
  apiKey: API_KEYS.OffsetFetch,
  apiVersion: 8,
  apiName: 'OffsetFetch',
  schema: requestSchema,
});
