import { createErrorFromCode, failure } from '../../../error-codes';
import { compactArray, compactString, defineResponse, field, flexibleObject, int16, int32 } from '../../../schema';
import type { AlterReplicaLogDirsResponseV0Body } from '../v0/response';

export type AlterReplicaLogDirsResponseV2Body = AlterReplicaLogDirsResponseV0Body;

/**
 * AlterReplicaLogDirs Response (Version: 2) => throttle_time_ms [results] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   results => topic [partitions] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partitions => partition error_code TAG_BUFFER
 *       partition => INT32
 *       error_code => INT16
 *
 * Flexible-version API. Response header v1's trailing TAG_BUFFER is skipped by `Connection`
 * before `decode()` runs.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = flexibleObject([field('partition', int32), field('errorCode', int16)]);
const resultSchema = flexibleObject([
  field('topic', compactString),
  field('partitions', compactArray(partitionSchema)),
]);
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('results', compactArray(resultSchema)),
]);

export const alterReplicaLogDirsResponseV2 = defineResponse<AlterReplicaLogDirsResponseV2Body>({
  schema: responseSchema,
  parse: async (data) => {
    const partitionWithError = data.results
      .flatMap((result) => result.partitions)
      .find((partition) => failure(partition.errorCode));
    if (partitionWithError) throw createErrorFromCode(partitionWithError.errorCode);
    return data;
  },
});
