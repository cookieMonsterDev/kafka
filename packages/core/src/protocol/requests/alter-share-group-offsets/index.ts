import type { ProtocolFactory, RequestFamily } from '../index';
import { alterShareGroupOffsetsRequestV0 } from './v0/request';
import { alterShareGroupOffsetsResponseV0 } from './v0/response';

export type { AlterShareGroupOffsetsResponseV0Body } from './v0/response';

export interface AlterShareGroupOffsetsPartitionInput {
  partitionIndex: number;
  startOffset: bigint;
}

export interface AlterShareGroupOffsetsTopicInput {
  topicName: string;
  partitions: AlterShareGroupOffsetsPartitionInput[];
}

export interface AlterShareGroupOffsetsOptions {
  groupId: string;
  topics: AlterShareGroupOffsetsTopicInput[];
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<AlterShareGroupOffsetsOptions>>> = {
  0: (options) => ({
    request: alterShareGroupOffsetsRequestV0({
      groupId: options.groupId,
      topics: options.topics.map(({ topicName, partitions }) => ({
        topicName,
        partitions: partitions.map(({ partitionIndex, startOffset }) => ({ partitionIndex, startOffset })),
      })),
    }),
    response: alterShareGroupOffsetsResponseV0,
  }),
};

export const AlterShareGroupOffsets: RequestFamily<AlterShareGroupOffsetsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no AlterShareGroupOffsets protocol for version ${version}`);
    return factory;
  },
});
