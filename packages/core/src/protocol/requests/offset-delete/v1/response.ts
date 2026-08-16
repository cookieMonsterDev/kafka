import { createErrorFromCode, failure } from '../../../error-codes';
import { compactArray, compactString, defineResponse, field, flexibleObject, int16, int32 } from '../../../schema';
import type { OffsetDeleteResponseV0Body } from '../v0/response';

export type OffsetDeleteResponseV1Body = OffsetDeleteResponseV0Body;

/**
 * OffsetDelete Response (Version: 1) => error_code throttle_time_ms [topics] TAG_BUFFER
 *   error_code => INT16
 *   throttle_time_ms => INT32
 *   topics => name [partitions] TAG_BUFFER
 *     name => COMPACT_STRING
 *     partitions => partition_index error_code TAG_BUFFER
 *       partition_index => INT32
 *       error_code => INT16
 *
 * Flexible-version API. Response header v1's trailing TAG_BUFFER is skipped by `Connection`
 * before `decode()` runs.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = flexibleObject([field('partitionIndex', int32), field('errorCode', int16)]);
const topicSchema = flexibleObject([field('name', compactString), field('partitions', compactArray(partitionSchema))]);
export const responseSchema = flexibleObject([
  field('errorCode', int16),
  field('throttleTime', int32),
  field('topics', compactArray(topicSchema)),
]);

export const offsetDeleteResponseV1 = defineResponse<OffsetDeleteResponseV1Body>({
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
