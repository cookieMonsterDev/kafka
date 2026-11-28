import {
  compactArray,
  compactNullableString,
  compactString,
  defineResponse,
  field,
  flexibleObject,
  int16,
  int32,
  int64,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { checkOffsetFetchGroupErrors } from '../shared';

export interface OffsetFetchPartitionV8 {
  partition: number;
  offset: bigint;
  leaderEpoch: number;
  metadata: string | null;
  errorCode: number;
}

export interface OffsetFetchResponseV8Body {
  groups: {
    groupId: string;
    topics: { topic: string; partitions: OffsetFetchPartitionV8[] }[];
    errorCode: number;
  }[];
  throttleTime: number;
  clientSideThrottleTime: number;
}

export const partitionSchema = flexibleObject([
  field('partition', int32),
  field('offset', int64),
  field('leaderEpoch', int32),
  field('metadata', compactNullableString),
  field('errorCode', int16),
]);
const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(partitionSchema))]);
const groupSchema = flexibleObject([
  field('groupId', compactString),
  field('topics', compactArray(topicSchema)),
  field('errorCode', int16),
]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('groups', compactArray(groupSchema))]);

const rawResponse = defineResponse({ schema: bodySchema });

/**
 * OffsetFetch Response (Version: 8) => throttle_time_ms [groups] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   groups => group_id [topics] error_code TAG_BUFFER
 *     group_id => COMPACT_STRING
 *     topics => name [partitions] TAG_BUFFER
 *       name => COMPACT_STRING
 *       partitions => partition_index committed_offset committed_leader_epoch metadata error_code TAG_BUFFER
 *         partition_index => INT32
 *         committed_offset => INT64
 *         committed_leader_epoch => INT32
 *         metadata => COMPACT_NULLABLE_STRING
 *         error_code => INT16
 *     error_code => INT16
 *
 * One entry per requested group (KIP-709); the error code moves from the top level to each group.
 * Quota timing follows v4 (KIP-219).
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const offsetFetchResponseV8: ResponseDefinition<OffsetFetchResponseV8Body> = {
  decode: async (rawData) => {
    const decoded = await rawResponse.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    checkOffsetFetchGroupErrors(data);
    return data;
  },
};
