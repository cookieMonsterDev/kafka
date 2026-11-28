import {
  boolean,
  compactArray,
  compactNullableString,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int32,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { compactNullableTopics } from '../v6/request';

const groupSchema = flexibleObject([
  field('groupId', compactString),
  field('memberId', compactNullableString),
  field('memberEpoch', int32),
  field('topics', compactNullableTopics),
]);

/**
 * OffsetFetch Request (Version: 9) => [groups] require_stable TAG_BUFFER
 *   groups => group_id member_id member_epoch [topics] TAG_BUFFER
 *     group_id => COMPACT_STRING
 *     member_id => COMPACT_NULLABLE_STRING
 *     member_epoch => INT32
 *     topics => name [partition_indexes] TAG_BUFFER
 *       name => COMPACT_STRING
 *       partition_indexes => INT32
 *   require_stable => BOOLEAN
 *
 * v8 plus each group's member id and member epoch (KIP-848). A consumer protocol member sends
 * them so the coordinator can answer STALE_MEMBER_EPOCH or UNKNOWN_MEMBER_ID; a null member id
 * and epoch `-1` skip that check.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('groups', compactArray(groupSchema)),
  field('requireStable', boolean),
]);

export const offsetFetchRequestV9 = defineRequest({
  apiKey: API_KEYS.OffsetFetch,
  apiVersion: 9,
  apiName: 'OffsetFetch',
  schema: requestSchema,
});
