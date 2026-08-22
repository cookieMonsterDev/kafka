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
  uuid,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import type { DescribeShareGroupOffsetsResponseV0Body } from '../v0/response';

export interface DescribeShareGroupOffsetsPartitionV1 {
  partitionIndex: number;
  startOffset: bigint;
  leaderEpoch: number;
  lag: bigint;
  errorCode: number;
  errorMessage: string | null;
}

export interface DescribeShareGroupOffsetsTopicV1 {
  topicName: string;
  topicId: Buffer;
  partitions: DescribeShareGroupOffsetsPartitionV1[];
}

export interface DescribeShareGroupOffsetsGroupV1 {
  groupId: string;
  topics: DescribeShareGroupOffsetsTopicV1[];
  errorCode: number;
  errorMessage: string | null;
}

export interface DescribeShareGroupOffsetsResponseV1Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  groups: DescribeShareGroupOffsetsGroupV1[];
}

/**
 * DescribeShareGroupOffsets Response (Version: 1) adds `lag` after leader_epoch (KIP-1226).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = flexibleObject([
  field('partitionIndex', int32),
  field('startOffset', int64),
  field('leaderEpoch', int32),
  field('lag', int64),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
]);
const topicSchema = flexibleObject([
  field('topicName', compactString),
  field('topicId', uuid),
  field('partitions', compactArray(partitionSchema)),
]);
const groupSchema = flexibleObject([
  field('groupId', compactString),
  field('topics', compactArray(topicSchema)),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
]);
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('groups', compactArray(groupSchema)),
]);

export const describeShareGroupOffsetsResponseV1: ResponseDefinition<DescribeShareGroupOffsetsResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    const failedGroup = data.groups.find(({ errorCode }) => failure(errorCode));
    if (failedGroup) throw createErrorFromCode(failedGroup.errorCode);
    const failedPartition = data.groups
      .flatMap(({ topics }) => topics)
      .flatMap(({ partitions }) => partitions)
      .find(({ errorCode }) => failure(errorCode));
    if (failedPartition) throw createErrorFromCode(failedPartition.errorCode);
    return data;
  },
};

export type { DescribeShareGroupOffsetsResponseV0Body };
