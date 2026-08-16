import type { ProtocolFactory, RequestFamily } from '../index';
import { type AlterConfigsResource, alterConfigsRequestV0 } from './v0/request';
import { alterConfigsResponseV0 } from './v0/response';
import { alterConfigsRequestV1 } from './v1/request';
import { alterConfigsResponseV1 } from './v1/response';
import { alterConfigsRequestV2 } from './v2/request';
import { alterConfigsResponseV2 } from './v2/response';

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
  2: (options) => ({
    request: alterConfigsRequestV2({ resources: options.resources, validateOnly: options.validateOnly ?? false }),
    response: alterConfigsResponseV2,
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
