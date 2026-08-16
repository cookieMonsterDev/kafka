import type { ProtocolFactory, RequestFamily } from '../index';
import type { CreatePartitionsTopicInput } from './v0/request';
import { createPartitionsRequestV0, withAssignmentDefaults } from './v0/request';
import { createPartitionsResponseV0 } from './v0/response';
import { createPartitionsRequestV1 } from './v1/request';
import { createPartitionsResponseV1 } from './v1/response';
import { createPartitionsRequestV2 } from './v2/request';
import { createPartitionsResponseV2 } from './v2/response';
import { createPartitionsRequestV3 } from './v3/request';
import { createPartitionsResponseV3 } from './v3/response';

export interface CreatePartitionsOptions {
  topicPartitions: CreatePartitionsTopicInput[];
  validateOnly?: boolean;
  timeout?: number;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<CreatePartitionsOptions>>> = {
  0: (options) => ({
    request: createPartitionsRequestV0({
      topicPartitions: withAssignmentDefaults(options.topicPartitions),
      timeout: options.timeout ?? 5000,
      validateOnly: options.validateOnly ?? false,
    }),
    response: createPartitionsResponseV0,
  }),
  1: (options) => ({
    request: createPartitionsRequestV1({
      topicPartitions: withAssignmentDefaults(options.topicPartitions),
      timeout: options.timeout ?? 5000,
      validateOnly: options.validateOnly ?? false,
    }),
    response: createPartitionsResponseV1,
  }),
  2: (options) => ({
    request: createPartitionsRequestV2({
      topicPartitions: withAssignmentDefaults(options.topicPartitions),
      timeout: options.timeout ?? 5000,
      validateOnly: options.validateOnly ?? false,
    }),
    response: createPartitionsResponseV2,
  }),
  3: (options) => ({
    request: createPartitionsRequestV3({
      topicPartitions: withAssignmentDefaults(options.topicPartitions),
      timeout: options.timeout ?? 5000,
      validateOnly: options.validateOnly ?? false,
    }),
    response: createPartitionsResponseV3,
  }),
};

export const CreatePartitions: RequestFamily<CreatePartitionsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no CreatePartitions protocol for version ${version}`);
    return factory;
  },
});
