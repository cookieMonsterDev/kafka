import type { ProtocolFactory, RequestFamily } from '../index';
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

const VERSIONS: Readonly<Record<number, ProtocolFactory<DescribeShareGroupOffsetsOptions>>> = {
  0: (options) => ({
    request: describeShareGroupOffsetsRequestV0({ groups: toRequestGroups(options.groups) }),
    response: describeShareGroupOffsetsResponseV0,
  }),
  1: (options) => ({
    request: describeShareGroupOffsetsRequestV1({ groups: toRequestGroups(options.groups) }),
    response: describeShareGroupOffsetsResponseV1,
  }),
};

export const DescribeShareGroupOffsets: RequestFamily<DescribeShareGroupOffsetsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) {
      throw new Error(`Invariant violated: no DescribeShareGroupOffsets protocol for version ${version}`);
    }
    return factory;
  },
});
