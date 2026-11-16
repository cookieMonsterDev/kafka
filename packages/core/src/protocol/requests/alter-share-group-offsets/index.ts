import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
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

const VERSIONS = {
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
} satisfies VersionFactories<AlterShareGroupOffsetsOptions>;

export const AlterShareGroupOffsets = defineRequestFamily(KAFKA_APIS.AlterShareGroupOffsets, VERSIONS);
