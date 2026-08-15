import type { ProtocolFactory, RequestFamily } from '../index.js';
import { RETENTION_TIME, type OffsetCommitTopicOptions, withDefaultMetadata } from './shared.js';
import { offsetCommitRequestV2 } from './v2/request.js';
import { offsetCommitResponseV2 } from './v2/response.js';
import { offsetCommitRequestV3 } from './v3/request.js';
import { offsetCommitResponseV3 } from './v3/response.js';
import { offsetCommitRequestV4 } from './v4/request.js';
import { offsetCommitResponseV4 } from './v4/response.js';
import { offsetCommitRequestV5 } from './v5/request.js';
import { offsetCommitResponseV5 } from './v5/response.js';

export interface OffsetCommitOptions {
  groupId: string;
  groupGenerationId: number;
  memberId: string;
  retentionTime?: bigint;
  topics: OffsetCommitTopicOptions[];
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<OffsetCommitOptions>>> = {
  2: ({ groupId, groupGenerationId, memberId, retentionTime = RETENTION_TIME, topics }) => ({
    request: offsetCommitRequestV2({
      groupId,
      groupGenerationId,
      memberId,
      retentionTime,
      topics: withDefaultMetadata(topics),
    }),
    response: offsetCommitResponseV2,
  }),
  3: ({ groupId, groupGenerationId, memberId, retentionTime = RETENTION_TIME, topics }) => ({
    request: offsetCommitRequestV3({
      groupId,
      groupGenerationId,
      memberId,
      retentionTime,
      topics: withDefaultMetadata(topics),
    }),
    response: offsetCommitResponseV3,
  }),
  4: ({ groupId, groupGenerationId, memberId, retentionTime = RETENTION_TIME, topics }) => ({
    request: offsetCommitRequestV4({
      groupId,
      groupGenerationId,
      memberId,
      retentionTime,
      topics: withDefaultMetadata(topics),
    }),
    response: offsetCommitResponseV4,
  }),
  5: ({ groupId, groupGenerationId, memberId, topics }) => ({
    request: offsetCommitRequestV5({ groupId, groupGenerationId, memberId, topics: withDefaultMetadata(topics) }),
    response: offsetCommitResponseV5,
  }),
};

export const OffsetCommit: RequestFamily<OffsetCommitOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no OffsetCommit protocol for version ${version}`);
    return factory;
  },
});
