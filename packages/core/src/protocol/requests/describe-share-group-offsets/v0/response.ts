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

export interface DescribeShareGroupOffsetsPartitionV0 {
  partitionIndex: number;
  startOffset: bigint;
  leaderEpoch: number;
  /**
   * The share-partition lag (KIP-1226). Version 0 has no such field on the wire, so it decodes as
   * -1 (unknown), the field's default in Kafka's `DescribeShareGroupOffsetsResponse.json`.
   */
  lag: bigint;
  errorCode: number;
  errorMessage: string | null;
}

export interface DescribeShareGroupOffsetsTopicV0 {
  topicName: string;
  topicId: Buffer;
  partitions: DescribeShareGroupOffsetsPartitionV0[];
}

export interface DescribeShareGroupOffsetsGroupV0 {
  groupId: string;
  topics: DescribeShareGroupOffsetsTopicV0[];
  errorCode: number;
  errorMessage: string | null;
}

export interface DescribeShareGroupOffsetsResponseV0Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  groups: DescribeShareGroupOffsetsGroupV0[];
}

/**
 * DescribeShareGroupOffsets Response (Version: 0) => throttle_time_ms [groups] TAG_BUFFER
 *
 * Quota timing follows KIP-219.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
const partitionSchema = flexibleObject([
  field('partitionIndex', int32),
  field('startOffset', int64),
  field('leaderEpoch', int32),
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

export const describeShareGroupOffsetsResponseV0: ResponseDefinition<DescribeShareGroupOffsetsResponseV0Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return {
      throttleTime: 0,
      clientSideThrottleTime: decoded.throttleTime,
      groups: decoded.groups.map((group) => ({
        ...group,
        topics: group.topics.map((topic) => ({
          ...topic,
          partitions: topic.partitions.map((partition) => ({ ...partition, lag: -1n })),
        })),
      })),
    };
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
