import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import {
  compactArray,
  compactNullableString,
  compactString,
  field,
  flexibleObject,
  int16,
  int32,
  int64,
  uint16,
  uuid,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface DescribeQuorumListenerV2 {
  name: string;
  host: string;
  port: number;
}

export interface DescribeQuorumNodeV2 {
  nodeId: number;
  listeners: DescribeQuorumListenerV2[];
}

export interface DescribeQuorumReplicaStateV2 {
  replicaId: number;
  replicaDirectoryId: Buffer;
  logEndOffset: bigint;
  lastFetchTimestamp: bigint;
  lastCaughtUpTimestamp: bigint;
}

export interface DescribeQuorumPartitionDataV2 {
  partitionIndex: number;
  errorCode: number;
  errorMessage: string | null;
  leaderId: number;
  leaderEpoch: number;
  highWatermark: bigint;
  currentVoters: DescribeQuorumReplicaStateV2[];
  observers: DescribeQuorumReplicaStateV2[];
}

export interface DescribeQuorumTopicDataV2 {
  topicName: string;
  partitions: DescribeQuorumPartitionDataV2[];
}

export interface DescribeQuorumResponseV2Body {
  errorCode: number;
  errorMessage: string | null;
  topics: DescribeQuorumTopicDataV2[];
  nodes: DescribeQuorumNodeV2[];
}

/**
 * DescribeQuorum Response (Version: 2) adds ErrorMessage, Nodes, partition ErrorMessage,
 * and ReplicaDirectoryId (KIP-853).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const replicaSchema = flexibleObject([
  field('replicaId', int32),
  field('replicaDirectoryId', uuid),
  field('logEndOffset', int64),
  field('lastFetchTimestamp', int64),
  field('lastCaughtUpTimestamp', int64),
]);
const partitionSchema = flexibleObject([
  field('partitionIndex', int32),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
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
const listenerSchema = flexibleObject([
  field('name', compactString),
  field('host', compactString),
  field('port', uint16),
]);
const nodeSchema = flexibleObject([field('nodeId', int32), field('listeners', compactArray(listenerSchema))]);
export const responseSchema = flexibleObject([
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('topics', compactArray(topicSchema)),
  field('nodes', compactArray(nodeSchema)),
]);

export const describeQuorumResponseV2: ResponseDefinition<DescribeQuorumResponseV2Body> = {
  decode: async (rawData) => responseSchema.read(new Decoder(rawData)),
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
