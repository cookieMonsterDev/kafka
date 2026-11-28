import { compactArray, compactString, defineRequest, field, flexibleObject, int32, object } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { emptyAsNullCompactArray } from '../shared';

/**
 * Partition indexes stay `[]int32` (no per-element TAG_BUFFER) — they are not a nested struct
 * in the official schema.
 */
export const partitionIndexesSchema = compactArray(object([field('partition', int32)]));
const topicSchema = flexibleObject([field('topic', compactString), field('partitions', partitionIndexesSchema)]);

export const compactNullableTopics = emptyAsNullCompactArray(topicSchema);

/**
 * OffsetFetch Request (Version: 6) => group_id [topics] TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   topics => topic [partitions] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partitions => partition
 *       partition => INT32
 *
 * First flexible version (KIP-482). Compact types + TAG_BUFFER on every struct. Empty `topics`
 * is written as a compact null ("all topics"). Request header v2's trailing TAG_BUFFER is
 * written by `createRequest`, not here.
 */
export const requestSchema = flexibleObject([field('groupId', compactString), field('topics', compactNullableTopics)]);

export const offsetFetchRequestV6 = defineRequest({
  apiKey: API_KEYS.OffsetFetch,
  apiVersion: 6,
  apiName: 'OffsetFetch',
  schema: requestSchema,
});
