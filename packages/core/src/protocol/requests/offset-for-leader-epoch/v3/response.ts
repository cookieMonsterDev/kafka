import { array, defineResponse, field, int16, int32, int64, object, string } from '../../../schema';
import { checkOffsetForLeaderEpochErrors } from '../shared';

export interface OffsetForLeaderEpochResponseV3Body {
  throttleTime: number;
  topics: {
    topic: string;
    partitions: { errorCode: number; partition: number; leaderEpoch: number; endOffset: bigint }[];
  }[];
}

/**
 * OffsetForLeaderEpoch Response (Version: 3) => throttle_time_ms [topics]
 *   throttle_time_ms => INT32
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => error_code partition leader_epoch end_offset
 *       error_code => INT16
 *       partition => INT32
 *       leader_epoch => INT32
 *       end_offset => INT64
 *
 * Wire format identical to v2 - only the request gains `replica_id` at this version.
 */
const partitionSchema = object([
  field('errorCode', int16),
  field('partition', int32),
  field('leaderEpoch', int32),
  field('endOffset', int64),
]);
const topicSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const bodySchema = object([field('throttleTime', int32), field('topics', array(topicSchema))]);

export const offsetForLeaderEpochResponseV3 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    checkOffsetForLeaderEpochErrors(data);
    return data;
  },
});
