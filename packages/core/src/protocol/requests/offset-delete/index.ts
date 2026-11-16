import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { offsetDeleteRequestV0 } from './v0/request';
import { offsetDeleteResponseV0 } from './v0/response';
import { offsetDeleteRequestV1 } from './v1/request';
import { offsetDeleteResponseV1 } from './v1/response';

export interface OffsetDeleteTopicInput {
  topic: string;
  partitions: number[];
}

export interface OffsetDeleteOptions {
  groupId: string;
  topics: OffsetDeleteTopicInput[];
}

function toRequestTopics(topics: OffsetDeleteTopicInput[]) {
  return topics.map(({ topic, partitions }) => ({
    name: topic,
    partitions: partitions.map((partitionIndex) => ({ partitionIndex })),
  }));
}

const VERSIONS = {
  0: (options) => ({
    request: offsetDeleteRequestV0({ groupId: options.groupId, topics: toRequestTopics(options.topics) }),
    response: offsetDeleteResponseV0,
  }),
  1: (options) => ({
    request: offsetDeleteRequestV1({ groupId: options.groupId, topics: toRequestTopics(options.topics) }),
    response: offsetDeleteResponseV1,
  }),
} satisfies VersionFactories<OffsetDeleteOptions>;

export const OffsetDelete = defineRequestFamily(KAFKA_APIS.OffsetDelete, VERSIONS);
