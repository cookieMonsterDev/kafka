import {
  compactArray,
  compactString,
  defineResponse,
  field,
  flexibleObject,
  int16,
  int32,
  int64,
} from '../../../schema';
import { checkOffsetForLeaderEpochErrors } from '../shared';

export interface OffsetForLeaderEpochResponseV3Body {
  throttleTime: number;
  topics: {
    topic: string;
    partitions: { errorCode: number; partition: number; leaderEpoch: number; endOffset: bigint }[];
  }[];
}

/**
 * OffsetForLeaderEpoch Response (Version: 3) => throttle_time_ms [topics] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   topics => topic [partitions] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partitions => error_code partition leader_epoch end_offset TAG_BUFFER
 *       error_code => INT16
 *       partition => INT32
 *       leader_epoch => INT32
 *       end_offset => INT64
 *
 * Flexible form of v2.
 */
const partitionSchema = flexibleObject([
  field('errorCode', int16),
  field('partition', int32),
  field('leaderEpoch', int32),
  field('endOffset', int64),
]);
const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(partitionSchema))]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('topics', compactArray(topicSchema))]);

export const offsetForLeaderEpochResponseV3 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    checkOffsetForLeaderEpochErrors(data);
    return data;
  },
});
