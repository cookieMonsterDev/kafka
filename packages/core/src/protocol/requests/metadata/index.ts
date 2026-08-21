import type { ProtocolFactory, RequestFamily } from '../index';
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
  allowAutoTopicCreation?: boolean;
  includeClusterAuthorizedOperations?: boolean;
  includeTopicAuthorizedOperations?: boolean;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<MetadataOptions>>> = {
  0: ({ topics = [] }) => ({ request: metadataRequestV0({ topics }), response: metadataResponseV0 }),
  1: ({ topics = [] }) => ({ request: metadataRequestV1({ topics }), response: metadataResponseV1 }),
  2: ({ topics = [] }) => ({ request: metadataRequestV2({ topics }), response: metadataResponseV2 }),
  3: ({ topics = [] }) => ({ request: metadataRequestV3({ topics }), response: metadataResponseV3 }),
  4: ({ topics = [], allowAutoTopicCreation = true }) => ({
    request: metadataRequestV4({ topics, allowAutoTopicCreation }),
    response: metadataResponseV4,
  }),
  5: ({ topics = [], allowAutoTopicCreation = true }) => ({
    request: metadataRequestV5({ topics, allowAutoTopicCreation }),
    response: metadataResponseV5,
  }),
  6: ({ topics = [], allowAutoTopicCreation = true }) => ({
    request: metadataRequestV6({ topics, allowAutoTopicCreation }),
    response: metadataResponseV6,
  }),
  7: ({ topics = [], allowAutoTopicCreation = true }) => ({
    request: metadataRequestV7({ topics, allowAutoTopicCreation }),
    response: metadataResponseV7,
  }),
  8: ({
    topics = [],
    allowAutoTopicCreation = true,
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
    allowAutoTopicCreation = true,
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
    allowAutoTopicCreation = true,
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
  11: ({ topics = [], topicIds, allowAutoTopicCreation = true, includeTopicAuthorizedOperations = false }) => ({
    request: metadataRequestV11({
      topics,
      topicIds,
      allowAutoTopicCreation,
      includeTopicAuthorizedOperations,
    }),
    response: metadataResponseV11,
  }),
  12: ({ topics = [], topicIds, allowAutoTopicCreation = true, includeTopicAuthorizedOperations = false }) => ({
    request: metadataRequestV12({
      topics,
      topicIds,
      allowAutoTopicCreation,
      includeTopicAuthorizedOperations,
    }),
    response: metadataResponseV12,
  }),
  13: ({ topics = [], topicIds, allowAutoTopicCreation = true, includeTopicAuthorizedOperations = false }) => ({
    request: metadataRequestV13({
      topics,
      topicIds,
      allowAutoTopicCreation,
      includeTopicAuthorizedOperations,
    }),
    response: metadataResponseV13,
  }),
};

export const Metadata: RequestFamily<MetadataOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no Metadata protocol for version ${version}`);
    return factory;
  },
});
