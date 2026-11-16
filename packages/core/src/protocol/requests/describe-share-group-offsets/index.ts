import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { describeShareGroupOffsetsRequestV0 } from './v0/request';
import { describeShareGroupOffsetsResponseV0 } from './v0/response';
import { describeShareGroupOffsetsRequestV1 } from './v1/request';
import { describeShareGroupOffsetsResponseV1 } from './v1/response';

export type { DescribeShareGroupOffsetsResponseV0Body } from './v0/response';
export type { DescribeShareGroupOffsetsResponseV1Body } from './v1/response';

export interface DescribeShareGroupOffsetsTopicInput {
  topicName: string;
  partitions: number[];
}

export interface DescribeShareGroupOffsetsGroupInput {
  groupId: string;
  topics?: DescribeShareGroupOffsetsTopicInput[] | null;
}

export interface DescribeShareGroupOffsetsOptions {
  groups: DescribeShareGroupOffsetsGroupInput[];
}

function toRequestGroups(groups: DescribeShareGroupOffsetsGroupInput[]) {
  return groups.map(({ groupId, topics }) => ({
    groupId,
    topics:
      topics == null
        ? null
        : topics.map(({ topicName, partitions }) => ({
            topicName,
            partitions,
          })),
  }));
}

const VERSIONS = {
  0: (options) => ({
    request: describeShareGroupOffsetsRequestV0({ groups: toRequestGroups(options.groups) }),
    response: describeShareGroupOffsetsResponseV0,
  }),
  1: (options) => ({
    request: describeShareGroupOffsetsRequestV1({ groups: toRequestGroups(options.groups) }),
    response: describeShareGroupOffsetsResponseV1,
  }),
} satisfies VersionFactories<DescribeShareGroupOffsetsOptions>;

export const DescribeShareGroupOffsets = defineRequestFamily(KAFKA_APIS.DescribeShareGroupOffsets, VERSIONS);
