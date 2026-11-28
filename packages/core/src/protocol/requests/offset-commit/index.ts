import { defineRequestFamily, highestVersionBelow, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { isUsableTopicId } from '../topic-ids';
import {
  OFFSET_COMMIT_TOPIC_ID_MIN_VERSION,
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
import { offsetCommitRequestV9 } from './v9/request';
import { offsetCommitResponseV9 } from './v9/response';
import { offsetCommitRequestV10 } from './v10/request';
import { offsetCommitResponseV10 } from './v10/response';

export interface OffsetCommitOptions {
  groupId: string;
  groupGenerationId: number;
  memberId: string;
  groupInstanceId?: string | null;
  retentionTime?: bigint;
  topics: OffsetCommitTopicOptions[];
}

const VERSIONS = {
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
  9: ({ groupId, groupGenerationId, memberId, groupInstanceId = null, topics }) => ({
    request: offsetCommitRequestV9({
      groupId,
      groupGenerationId,
      memberId,
      groupInstanceId,
      topics: withDefaultMetadata(topics),
    }),
    response: offsetCommitResponseV9,
  }),
  10: ({ groupId, groupGenerationId, memberId, groupInstanceId = null, topics }) => ({
    request: offsetCommitRequestV10({ groupId, groupGenerationId, memberId, groupInstanceId, topics }),
    response: offsetCommitResponseV10(topics),
  }),
} satisfies VersionFactories<OffsetCommitOptions>;

const OFFSET_COMMIT_VERSIONS: readonly number[] = Object.keys(VERSIONS).map(Number);

/**
 * v9 is the first version the consumer protocol (KIP-848) commits with; `groupGenerationId` then
 * carries the member epoch. v10 addresses topics by id; a commit that cannot name every topic by
 * id stays on the highest name-based version (v9).
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const OffsetCommit = defineRequestFamily(KAFKA_APIS.OffsetCommit, VERSIONS, {
  preferVersion: (options, version, advertised) =>
    version < OFFSET_COMMIT_TOPIC_ID_MIN_VERSION || options.topics.every(({ topicId }) => isUsableTopicId(topicId))
      ? version
      : highestVersionBelow(
          KAFKA_APIS.OffsetCommit,
          OFFSET_COMMIT_VERSIONS,
          OFFSET_COMMIT_TOPIC_ID_MIN_VERSION,
          advertised,
        ),
});
