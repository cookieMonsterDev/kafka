import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { deleteShareGroupOffsetsRequestV0 } from './v0/request';
import { deleteShareGroupOffsetsResponseV0 } from './v0/response';

export type { DeleteShareGroupOffsetsResponseV0Body } from './v0/response';

export interface DeleteShareGroupOffsetsOptions {
  groupId: string;
  topics: string[];
}

const VERSIONS = {
  0: (options) => ({
    request: deleteShareGroupOffsetsRequestV0({
      groupId: options.groupId,
      topics: options.topics.map((topicName) => ({ topicName })),
    }),
    response: deleteShareGroupOffsetsResponseV0,
  }),
} satisfies VersionFactories<DeleteShareGroupOffsetsOptions>;

export const DeleteShareGroupOffsets = defineRequestFamily(KAFKA_APIS.DeleteShareGroupOffsets, VERSIONS);
