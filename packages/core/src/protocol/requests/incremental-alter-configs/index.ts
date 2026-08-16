import type { ProtocolFactory, RequestFamily } from '../index';
import { type IncrementalAlterConfigsResource, incrementalAlterConfigsRequestV0 } from './v0/request';
import { incrementalAlterConfigsResponseV0 } from './v0/response';
import { incrementalAlterConfigsRequestV1 } from './v1/request';
import { incrementalAlterConfigsResponseV1 } from './v1/response';

export interface IncrementalAlterConfigsOptions {
  resources: IncrementalAlterConfigsResource[];
  validateOnly?: boolean;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<IncrementalAlterConfigsOptions>>> = {
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
};

export const IncrementalAlterConfigs: RequestFamily<IncrementalAlterConfigsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no IncrementalAlterConfigs protocol for version ${version}`);
    return factory;
  },
});
