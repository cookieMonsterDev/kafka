import { array, defineResponse, field, int16, int32, int64, object, string } from '../../../schema';
import { checkOffsetForLeaderEpochErrors } from '../shared';

export interface OffsetForLeaderEpochResponseV0Body {
  topics: {
    topic: string;
    partitions: { errorCode: number; partition: number; endOffset: bigint }[];
  }[];
}

/**
 * OffsetForLeaderEpoch Response (Version: 0) => [topics]
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => error_code partition end_offset
 *       error_code => INT16
 *       partition => INT32
 *       end_offset => INT64
 */
const partitionSchema = object([field('errorCode', int16), field('partition', int32), field('endOffset', int64)]);
const topicSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const bodySchema = object([field('topics', array(topicSchema))]);

export const offsetForLeaderEpochResponseV0 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    checkOffsetForLeaderEpochErrors(data);
    return data;
  },
});
