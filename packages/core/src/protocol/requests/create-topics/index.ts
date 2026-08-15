import type { ProtocolFactory, RequestFamily } from '../index.js';
import type { CreateTopicInput } from './v2/request.js';
import { createTopicsRequestV2, withTopicDefaults } from './v2/request.js';
import { createTopicsResponseV2 } from './v2/response.js';
import { createTopicsRequestV3 } from './v3/request.js';
import { createTopicsResponseV3 } from './v3/response.js';

export interface CreateTopicsOptions {
  topics: CreateTopicInput[];
  validateOnly?: boolean;
  timeout?: number;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<CreateTopicsOptions>>> = {
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
