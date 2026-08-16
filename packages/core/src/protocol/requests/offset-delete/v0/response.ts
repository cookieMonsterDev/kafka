import { createErrorFromCode, failure } from '../../../error-codes';
import { array, defineResponse, field, int16, int32, object, string } from '../../../schema';

export interface OffsetDeletePartitionResult {
  partitionIndex: number;
  errorCode: number;
}

export interface OffsetDeleteTopicResult {
  name: string;
  partitions: OffsetDeletePartitionResult[];
}

export interface OffsetDeleteResponseV0Body {
  errorCode: number;
  throttleTime: number;
  topics: OffsetDeleteTopicResult[];
}

/**
 * OffsetDelete Response (Version: 0) => error_code throttle_time_ms [topics]
 *   error_code => INT16
 *   throttle_time_ms => INT32
 *   topics => name [partitions]
 *     name => STRING
 *     partitions => partition_index error_code
 *       partition_index => INT32
 *       error_code => INT16
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = object([field('partitionIndex', int32), field('errorCode', int16)]);
const topicSchema = object([field('name', string), field('partitions', array(partitionSchema))]);
export const responseSchema = object([
  field('errorCode', int16),
  field('throttleTime', int32),
  field('topics', array(topicSchema)),
]);

export const offsetDeleteResponseV0 = defineResponse<OffsetDeleteResponseV0Body>({
  schema: responseSchema,
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    const partitionWithError = data.topics
      .flatMap((topic) => topic.partitions)
      .find((partition) => failure(partition.errorCode));
    if (partitionWithError) throw createErrorFromCode(partitionWithError.errorCode);
    return data;
  },
});
