import type { ProtocolFactory, RequestFamily } from '../index';
import {
  RETENTION_TIME,
  type OffsetCommitTopicOptions,
  withDefaultMetadata,
  withDefaultMetadataAndTimestamp,
} from './shared';
import { offsetCommitRequestV0 } from './v0/request';
import { offsetCommitResponseV0 } from './v0/response';
import { offsetCommitRequestV1 } from './v1/request';
import { offsetCommitResponseV1 } from './v1/response';
import { offsetCommitRequestV2 } from './v2/request';
import { offsetCommitResponseV2 } from './v2/response';
import { offsetCommitRequestV3 } from './v3/request';
import { offsetCommitResponseV3 } from './v3/response';
import { offsetCommitRequestV4 } from './v4/request';
import { offsetCommitResponseV4 } from './v4/response';
import { offsetCommitRequestV5 } from './v5/request';
import { offsetCommitResponseV5 } from './v5/response';
import { offsetCommitRequestV6 } from './v6/request';
import { offsetCommitResponseV6 } from './v6/response';
import { offsetCommitRequestV7 } from './v7/request';
import { offsetCommitResponseV7 } from './v7/response';
import { offsetCommitRequestV8 } from './v8/request';
import { offsetCommitResponseV8 } from './v8/response';

export interface OffsetCommitOptions {
  groupId: string;
  groupGenerationId: number;
  memberId: string;
  groupInstanceId?: string | null;
  retentionTime?: bigint;
  topics: OffsetCommitTopicOptions[];
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<OffsetCommitOptions>>> = {
  0: ({ groupId, topics }) => ({
    request: offsetCommitRequestV0({ groupId, topics: withDefaultMetadata(topics) }),
    response: offsetCommitResponseV0,
  }),
  1: ({ groupId, groupGenerationId, memberId, topics }) => ({
    request: offsetCommitRequestV1({
      groupId,
      groupGenerationId,
      memberId,
      topics: withDefaultMetadataAndTimestamp(topics),
    }),
    response: offsetCommitResponseV1,
  }),
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
  6: ({ groupId, groupGenerationId, memberId, topics }) => ({
    request: offsetCommitRequestV6({ groupId, groupGenerationId, memberId, topics: withDefaultMetadata(topics) }),
    response: offsetCommitResponseV6,
  }),
  7: ({ groupId, groupGenerationId, memberId, groupInstanceId = null, topics }) => ({
    request: offsetCommitRequestV7({
      groupId,
      groupGenerationId,
      memberId,
      groupInstanceId,
      topics: withDefaultMetadata(topics),
    }),
    response: offsetCommitResponseV7,
  }),
  8: ({ groupId, groupGenerationId, memberId, groupInstanceId = null, topics }) => ({
    request: offsetCommitRequestV8({
      groupId,
      groupGenerationId,
      memberId,
      groupInstanceId,
      topics: withDefaultMetadata(topics),
    }),
    response: offsetCommitResponseV8,
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
