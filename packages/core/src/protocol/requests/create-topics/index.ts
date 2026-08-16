import { KafkaNonRetriableError } from '../../../errors';
import type { ProtocolFactory, RequestFamily } from '../index';
import type { CreateTopicInput } from './v2/request';
import { createTopicsRequestV0 } from './v0/request';
import { createTopicsResponseV0 } from './v0/response';
import { createTopicsRequestV1 } from './v1/request';
import { createTopicsResponseV1 } from './v1/response';
import { createTopicsRequestV2, withTopicDefaults } from './v2/request';
import { createTopicsResponseV2 } from './v2/response';
import { createTopicsRequestV3 } from './v3/request';
import { createTopicsResponseV3 } from './v3/response';

export interface CreateTopicsOptions {
  topics: CreateTopicInput[];
  validateOnly?: boolean;
  timeout?: number;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<CreateTopicsOptions>>> = {
  0: (options) => {
    if (options.validateOnly === true) {
      throw new KafkaNonRetriableError(
        'CreateTopics v0 does not support validateOnly; this broker needs Kafka 0.11+ (CreateTopics v1)',
      );
    }
    return {
      request: createTopicsRequestV0({
        topics: withTopicDefaults(options.topics),
        timeout: options.timeout ?? 5000,
      }),
      response: createTopicsResponseV0,
    };
  },
  1: (options) => ({
    request: createTopicsRequestV1({
      topics: withTopicDefaults(options.topics),
      timeout: options.timeout ?? 5000,
      validateOnly: options.validateOnly ?? false,
    }),
    response: createTopicsResponseV1,
  }),
  2: (options) => ({
    request: createTopicsRequestV2({
      topics: withTopicDefaults(options.topics),
      timeout: options.timeout ?? 5000,
      validateOnly: options.validateOnly ?? false,
    }),
    response: createTopicsResponseV2,
  }),
  3: (options) => ({
    request: createTopicsRequestV3({
      topics: withTopicDefaults(options.topics),
      timeout: options.timeout ?? 5000,
      validateOnly: options.validateOnly ?? false,
    }),
    response: createTopicsResponseV3,
  }),
};

export const CreateTopics: RequestFamily<CreateTopicsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no CreateTopics protocol for version ${version}`);
    return factory;
  },
});
