import { createErrorFromCode, failure } from '../../../error-codes';
import { array, defineResponse, field, int16, int32, object, string } from '../../../schema';

export interface AlterReplicaLogDirPartitionResult {
  partition: number;
  errorCode: number;
}

export interface AlterReplicaLogDirTopicResult {
  topic: string;
  partitions: AlterReplicaLogDirPartitionResult[];
}

export interface AlterReplicaLogDirsResponseV0Body {
  throttleTime: number;
  results: AlterReplicaLogDirTopicResult[];
}

/**
 * AlterReplicaLogDirs Response (Version: 0) => throttle_time_ms [results]
 *   throttle_time_ms => INT32
 *   results => topic [partitions]
 *     topic => STRING
 *     partitions => partition error_code
 *       partition => INT32
 *       error_code => INT16
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = object([field('partition', int32), field('errorCode', int16)]);
const resultSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
export const responseSchema = object([field('throttleTime', int32), field('results', array(resultSchema))]);

export const alterReplicaLogDirsResponseV0 = defineResponse<AlterReplicaLogDirsResponseV0Body>({
  schema: responseSchema,
  parse: async (data) => {
    const partitionWithError = data.results
      .flatMap((result) => result.partitions)
      .find((partition) => failure(partition.errorCode));
    if (partitionWithError) throw createErrorFromCode(partitionWithError.errorCode);
    return data;
  },
});
