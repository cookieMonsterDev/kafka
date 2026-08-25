import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { compactArray, field, flexibleObject, int16, int32, uuid } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface AssignReplicasToDirsResponsePartition {
  partitionIndex: number;
  errorCode: number;
}

export interface AssignReplicasToDirsResponseTopic {
  topicId: Buffer;
  partitions: AssignReplicasToDirsResponsePartition[];
}

export interface AssignReplicasToDirsResponseDirectory {
  id: Buffer;
  topics: AssignReplicasToDirsResponseTopic[];
}

export interface AssignReplicasToDirsResponseV0Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  errorCode: number;
  directories: AssignReplicasToDirsResponseDirectory[];
}

/**
 * AssignReplicasToDirs Response (Version: 0) => throttle_time_ms error_code [directories] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   directories => id [topics] TAG_BUFFER
 *     id => UUID
 *     topics => topic_id [partitions] TAG_BUFFER
 *       topic_id => UUID
 *       partitions => partition_index error_code TAG_BUFFER
 *         partition_index => INT32
 *         error_code => INT16
 *
 * Flexible from v0. Quota timing follows KIP-219: the decoded throttle is exposed as
 * `clientSideThrottleTime`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = flexibleObject([field('partitionIndex', int32), field('errorCode', int16)]);
const topicSchema = flexibleObject([field('topicId', uuid), field('partitions', compactArray(partitionSchema))]);
const directorySchema = flexibleObject([field('id', uuid), field('topics', compactArray(topicSchema))]);
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('directories', compactArray(directorySchema)),
]);

export const assignReplicasToDirsResponseV0: ResponseDefinition<AssignReplicasToDirsResponseV0Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    const partitionWithError = data.directories
      .flatMap((directory) => directory.topics)
      .flatMap((topic) => topic.partitions)
      .find((partition) => failure(partition.errorCode));
    if (partitionWithError) throw createErrorFromCode(partitionWithError.errorCode);
    return data;
  },
};
