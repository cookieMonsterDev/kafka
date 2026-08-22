import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { compactArray, compactString, field, flexibleObject, int16, int32, int64 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface DescribeQuorumReplicaState {
  replicaId: number;
  logEndOffset: bigint;
}

export interface DescribeQuorumPartitionData {
  partitionIndex: number;
  errorCode: number;
  leaderId: number;
  leaderEpoch: number;
  highWatermark: bigint;
  currentVoters: DescribeQuorumReplicaState[];
  observers: DescribeQuorumReplicaState[];
}

export interface DescribeQuorumTopicData {
  topicName: string;
  partitions: DescribeQuorumPartitionData[];
}

export interface DescribeQuorumResponseV0Body {
  errorCode: number;
  topics: DescribeQuorumTopicData[];
}

/**
 * DescribeQuorum Response (Version: 0) => error_code [topics] TAG_BUFFER
 *   error_code => INT16
 *   topics => topic_name [partitions] TAG_BUFFER
 *     topic_name => COMPACT_STRING
 *     partitions => partition_index error_code leader_id leader_epoch high_watermark
 *                   [current_voters] [observers] TAG_BUFFER
 *       partition_index => INT32
 *       error_code => INT16
 *       leader_id => INT32
 *       leader_epoch => INT32
 *       high_watermark => INT64
 *       current_voters => replica_id log_end_offset TAG_BUFFER
 *         replica_id => INT32
 *         log_end_offset => INT64
 *       observers => replica_id log_end_offset TAG_BUFFER
 *
 * Flexible from v0. Response header v1's trailing TAG_BUFFER is skipped by `Connection`
 * before `decode()` runs.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const replicaSchema = flexibleObject([field('replicaId', int32), field('logEndOffset', int64)]);
const partitionSchema = flexibleObject([
  field('partitionIndex', int32),
  field('errorCode', int16),
  field('leaderId', int32),
  field('leaderEpoch', int32),
  field('highWatermark', int64),
  field('currentVoters', compactArray(replicaSchema)),
  field('observers', compactArray(replicaSchema)),
]);
const topicSchema = flexibleObject([
  field('topicName', compactString),
  field('partitions', compactArray(partitionSchema)),
]);
export const responseSchema = flexibleObject([field('errorCode', int16), field('topics', compactArray(topicSchema))]);

export const describeQuorumResponseV0: ResponseDefinition<DescribeQuorumResponseV0Body> = {
  decode: async (rawData) => responseSchema.read(new Decoder(rawData)),
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
