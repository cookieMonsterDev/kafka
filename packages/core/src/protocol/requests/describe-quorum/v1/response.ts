import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { compactArray, compactString, field, flexibleObject, int16, int32, int64 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface DescribeQuorumReplicaStateV1 {
  replicaId: number;
  logEndOffset: bigint;
  lastFetchTimestamp: bigint;
  lastCaughtUpTimestamp: bigint;
}

export interface DescribeQuorumPartitionDataV1 {
  partitionIndex: number;
  errorCode: number;
  leaderId: number;
  leaderEpoch: number;
  highWatermark: bigint;
  currentVoters: DescribeQuorumReplicaStateV1[];
  observers: DescribeQuorumReplicaStateV1[];
}

export interface DescribeQuorumTopicDataV1 {
  topicName: string;
  partitions: DescribeQuorumPartitionDataV1[];
}

export interface DescribeQuorumResponseV1Body {
  errorCode: number;
  topics: DescribeQuorumTopicDataV1[];
}

/**
 * DescribeQuorum Response (Version: 1) adds LastFetchTimestamp and LastCaughtUpTimestamp
 * to each replica (KIP-836).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const replicaSchema = flexibleObject([
  field('replicaId', int32),
  field('logEndOffset', int64),
  field('lastFetchTimestamp', int64),
  field('lastCaughtUpTimestamp', int64),
]);
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

export const describeQuorumResponseV1: ResponseDefinition<DescribeQuorumResponseV1Body> = {
  decode: async (rawData) => responseSchema.read(new Decoder(rawData)),
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
