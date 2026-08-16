import type { ProtocolFactory, RequestFamily } from '../index';
import { RETENTION_TIME, type OffsetCommitTopicOptions, withDefaultMetadata } from './shared';
import { offsetCommitRequestV2 } from './v2/request';
import { offsetCommitResponseV2 } from './v2/response';
import { offsetCommitRequestV3 } from './v3/request';
import { offsetCommitResponseV3 } from './v3/response';
import { offsetCommitRequestV4 } from './v4/request';
import { offsetCommitResponseV4 } from './v4/response';
import { offsetCommitRequestV5 } from './v5/request';
import { offsetCommitResponseV5 } from './v5/response';

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
