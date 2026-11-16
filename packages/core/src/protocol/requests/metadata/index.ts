import { shuffle } from '../../../utils/shuffle';
import { failure } from '../../error-codes';
import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import type { ClusterMetadata } from './shared';
import { metadataRequestV0 } from './v0/request';
import { metadataResponseV0 } from './v0/response';
import { metadataRequestV1 } from './v1/request';
import { metadataResponseV1 } from './v1/response';
import { metadataRequestV2 } from './v2/request';
import { metadataResponseV2 } from './v2/response';
import { metadataRequestV3 } from './v3/request';
import { metadataResponseV3 } from './v3/response';
import { metadataRequestV4 } from './v4/request';
import { metadataResponseV4 } from './v4/response';
import { metadataRequestV5 } from './v5/request';
import { metadataResponseV5 } from './v5/response';
import { metadataRequestV6 } from './v6/request';
import { metadataResponseV6 } from './v6/response';
import { metadataRequestV7 } from './v7/request';
import { metadataResponseV7 } from './v7/response';
import { metadataRequestV8 } from './v8/request';
import { metadataResponseV8 } from './v8/response';
import { metadataRequestV9 } from './v9/request';
import { metadataResponseV9 } from './v9/response';
import { metadataRequestV10 } from './v10/request';
import { metadataResponseV10 } from './v10/response';
import { metadataRequestV11 } from './v11/request';
import { metadataResponseV11 } from './v11/response';
import { metadataRequestV12 } from './v12/request';
import { metadataResponseV12 } from './v12/response';
import { metadataRequestV13 } from './v13/request';
import { metadataResponseV13 } from './v13/response';

export interface MetadataOptions {
  topics?: string[];
  topicIds?: Buffer[];
  /**
   * Whether the broker may auto-create requested topics (v4+). Required, with no family default:
   * the wire default is `true`, so a call that left it out would auto-create topics regardless of
   * the client's configuration. Pass the broker's configured `allowAutoTopicCreation`.
   */
  allowAutoTopicCreation: boolean;
  includeClusterAuthorizedOperations?: boolean;
  includeTopicAuthorizedOperations?: boolean;
}

const VERSIONS = {
  0: ({ topics = [] }) => ({ request: metadataRequestV0({ topics }), response: metadataResponseV0 }),
  1: ({ topics = [] }) => ({ request: metadataRequestV1({ topics }), response: metadataResponseV1 }),
  2: ({ topics = [] }) => ({ request: metadataRequestV2({ topics }), response: metadataResponseV2 }),
  3: ({ topics = [] }) => ({ request: metadataRequestV3({ topics }), response: metadataResponseV3 }),
  4: ({ topics = [], allowAutoTopicCreation }) => ({
    request: metadataRequestV4({ topics, allowAutoTopicCreation }),
    response: metadataResponseV4,
  }),
  5: ({ topics = [], allowAutoTopicCreation }) => ({
    request: metadataRequestV5({ topics, allowAutoTopicCreation }),
    response: metadataResponseV5,
  }),
  6: ({ topics = [], allowAutoTopicCreation }) => ({
    request: metadataRequestV6({ topics, allowAutoTopicCreation }),
    response: metadataResponseV6,
  }),
  7: ({ topics = [], allowAutoTopicCreation }) => ({
    request: metadataRequestV7({ topics, allowAutoTopicCreation }),
    response: metadataResponseV7,
  }),
  8: ({
    topics = [],
    allowAutoTopicCreation,
    includeClusterAuthorizedOperations = false,
    includeTopicAuthorizedOperations = false,
  }) => ({
    request: metadataRequestV8({
      topics,
      allowAutoTopicCreation,
      includeClusterAuthorizedOperations,
      includeTopicAuthorizedOperations,
    }),
    response: metadataResponseV8,
  }),
  9: ({
    topics = [],
    allowAutoTopicCreation,
    includeClusterAuthorizedOperations = false,
    includeTopicAuthorizedOperations = false,
  }) => ({
    request: metadataRequestV9({
      topics,
      allowAutoTopicCreation,
      includeClusterAuthorizedOperations,
      includeTopicAuthorizedOperations,
    }),
    response: metadataResponseV9,
  }),
  10: ({
    topics = [],
    topicIds,
    allowAutoTopicCreation,
    includeClusterAuthorizedOperations = false,
    includeTopicAuthorizedOperations = false,
  }) => ({
    request: metadataRequestV10({
      topics,
      topicIds,
      allowAutoTopicCreation,
      includeClusterAuthorizedOperations,
      includeTopicAuthorizedOperations,
    }),
    response: metadataResponseV10,
  }),
  11: ({ topics = [], topicIds, allowAutoTopicCreation, includeTopicAuthorizedOperations = false }) => ({
    request: metadataRequestV11({
      topics,
      topicIds,
      allowAutoTopicCreation,
      includeTopicAuthorizedOperations,
    }),
    response: metadataResponseV11,
  }),
  12: ({ topics = [], topicIds, allowAutoTopicCreation, includeTopicAuthorizedOperations = false }) => ({
    request: metadataRequestV12({
      topics,
      topicIds,
      allowAutoTopicCreation,
      includeTopicAuthorizedOperations,
    }),
    response: metadataResponseV12,
  }),
  13: ({ topics = [], topicIds, allowAutoTopicCreation, includeTopicAuthorizedOperations = false }) => ({
    request: metadataRequestV13({
      topics,
      topicIds,
      allowAutoTopicCreation,
      includeTopicAuthorizedOperations,
    }),
    response: metadataResponseV13,
  }),
} satisfies VersionFactories<MetadataOptions>;

/**
 * Asked for every topic, topics deleted or still being created in the meantime can carry
 * UNKNOWN_TOPIC_OR_PARTITION; leave those out rather than failing the whole response.
 */
function withoutFailedTopics(body: ClusterMetadata): ClusterMetadata {
  const topicMetadata = body.topicMetadata.filter(
    (topic) =>
      !failure(topic.topicErrorCode) &&
      !topic.partitionMetadata.some((partition) => failure(partition.partitionErrorCode)),
  );
  return { ...body, topicMetadata };
}

export const Metadata = defineRequestFamily(KAFKA_APIS.Metadata, VERSIONS, {
  prepareOptions: (options) => ({ ...options, topics: shuffle(options.topics ?? []) }),
  parseResponse: async (body, options, parse) =>
    // No topic names and no topic ids asks for every topic.
    options.topics?.length === 0 && (options.topicIds ?? []).length === 0
      ? withoutFailedTopics(body as ClusterMetadata)
      : ((await parse(body)) as ClusterMetadata),
});
