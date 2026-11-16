import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { type IncrementalAlterConfigsResource, incrementalAlterConfigsRequestV0 } from './v0/request';
import { incrementalAlterConfigsResponseV0 } from './v0/response';
import { incrementalAlterConfigsRequestV1 } from './v1/request';
import { incrementalAlterConfigsResponseV1 } from './v1/response';

export interface IncrementalAlterConfigsOptions {
  resources: IncrementalAlterConfigsResource[];
  validateOnly?: boolean;
}

const VERSIONS = {
  0: (options) => ({
    request: incrementalAlterConfigsRequestV0({
      resources: options.resources,
      validateOnly: options.validateOnly ?? false,
    }),
    response: incrementalAlterConfigsResponseV0,
  }),
  1: (options) => ({
    request: incrementalAlterConfigsRequestV1({
      resources: options.resources,
      validateOnly: options.validateOnly ?? false,
    }),
    response: incrementalAlterConfigsResponseV1,
  }),
} satisfies VersionFactories<IncrementalAlterConfigsOptions>;

export const IncrementalAlterConfigs = defineRequestFamily(KAFKA_APIS.IncrementalAlterConfigs, VERSIONS);
