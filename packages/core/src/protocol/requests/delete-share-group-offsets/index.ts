import type { ProtocolFactory, RequestFamily } from '../index';
import { deleteShareGroupOffsetsRequestV0 } from './v0/request';
import { deleteShareGroupOffsetsResponseV0 } from './v0/response';

export type { DeleteShareGroupOffsetsResponseV0Body } from './v0/response';

export interface DeleteShareGroupOffsetsOptions {
  groupId: string;
  topics: string[];
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<DeleteShareGroupOffsetsOptions>>> = {
  0: (options) => ({
    request: deleteShareGroupOffsetsRequestV0({
      groupId: options.groupId,
      topics: options.topics.map((topicName) => ({ topicName })),
    }),
    response: deleteShareGroupOffsetsResponseV0,
  }),
};

export const DeleteShareGroupOffsets: RequestFamily<DeleteShareGroupOffsetsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no DeleteShareGroupOffsets protocol for version ${version}`);
    return factory;
  },
});
