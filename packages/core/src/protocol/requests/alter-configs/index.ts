import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
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

const VERSIONS = {
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
} satisfies VersionFactories<AlterConfigsOptions>;

export const AlterConfigs = defineRequestFamily(KAFKA_APIS.AlterConfigs, VERSIONS);
