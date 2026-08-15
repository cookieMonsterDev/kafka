import type { ProtocolFactory, RequestFamily } from '../index.js';
import { type AlterConfigsResource, alterConfigsRequestV0 } from './v0/request.js';
import { alterConfigsResponseV0 } from './v0/response.js';
import { alterConfigsRequestV1 } from './v1/request.js';
import { alterConfigsResponseV1 } from './v1/response.js';

export interface AlterConfigsOptions {
  resources: AlterConfigsResource[];
  validateOnly?: boolean;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<AlterConfigsOptions>>> = {
  0: (options) => ({
    request: alterConfigsRequestV0({ resources: options.resources, validateOnly: options.validateOnly ?? false }),
    response: alterConfigsResponseV0,
  }),
  1: (options) => ({
    request: alterConfigsRequestV1({ resources: options.resources, validateOnly: options.validateOnly ?? false }),
    response: alterConfigsResponseV1,
  }),
};

export const AlterConfigs: RequestFamily<AlterConfigsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no AlterConfigs protocol for version ${version}`);
    return factory;
  },
});
