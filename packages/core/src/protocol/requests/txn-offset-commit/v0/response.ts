import { createErrorFromCode, failure } from '../../../error-codes.js';
import { array, defineResponse, field, int16, int32, object, string } from '../../../schema.js';

/**
 * TxnOffsetCommit Response (Version: 0) => throttle_time_ms [topics]
 *   throttle_time_ms => INT32
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition error_code
 *       partition => INT32
 *       error_code => INT16
 */
const partitionSchema = object([field('partition', int32), field('errorCode', int16)]);
const topicSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const bodySchema = object([field('throttleTime', int32), field('topics', array(topicSchema))]);

export const txnOffsetCommitResponseV0 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    for (const { partitions } of data.topics) {
      const failed = partitions.find((partition) => failure(partition.errorCode));
      if (failed) throw createErrorFromCode(failed.errorCode);
    }
    return data;
  },
});
