import {
  compactArray,
  compactNullableString,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int32,
  int64,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * OffsetCommit Request (Version: 8) => group_id generation_id member_id group_instance_id [topics] TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   generation_id => INT32
 *   member_id => COMPACT_STRING
 *   group_instance_id => COMPACT_NULLABLE_STRING
 *   topics => topic [partitions] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partitions => partition offset committed_leader_epoch metadata TAG_BUFFER
 *       partition => INT32
 *       offset => INT64
 *       committed_leader_epoch => INT32
 *       metadata => COMPACT_NULLABLE_STRING
 *
 * First flexible version (KIP-482). Same fields as v7; compact types + TAG_BUFFER on every struct.
 * Request header v2's trailing TAG_BUFFER is written by `createRequest`, not here.
 */
const partitionSchema = flexibleObject([
  field('partition', int32),
  field('offset', int64),
  field('leaderEpoch', int32),
  field('metadata', compactNullableString),
]);
const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(partitionSchema))]);
export const requestSchema = flexibleObject([
  field('groupId', compactString),
  field('groupGenerationId', int32),
  field('memberId', compactString),
  field('groupInstanceId', compactNullableString),
  field('topics', compactArray(topicSchema)),
]);

export const offsetCommitRequestV8 = defineRequest({
  apiKey: API_KEYS.OffsetCommit,
  apiVersion: 8,
  apiName: 'OffsetCommit',
  schema: requestSchema,
});
