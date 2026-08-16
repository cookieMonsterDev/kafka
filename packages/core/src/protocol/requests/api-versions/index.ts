import type { ProtocolFactory, RequestFamily } from '../index';
import { apiVersionsRequestV0 } from './v0/request';
import { apiVersionsResponseV0 } from './v0/response';
import { apiVersionsRequestV1 } from './v1/request';
import { apiVersionsResponseV1 } from './v1/response';
import { apiVersionsRequestV2 } from './v2/request';
import { apiVersionsResponseV2 } from './v2/response';
import { apiVersionsRequestV3, type ApiVersionsRequestOptions } from './v3/request';
import { apiVersionsResponseV3 } from './v3/response';

export type { ApiVersionsRequestOptions };

const VERSIONS: Readonly<Record<number, ProtocolFactory<ApiVersionsRequestOptions>>> = {
  0: () => ({ request: apiVersionsRequestV0({}), response: apiVersionsResponseV0, logResponseError: true }),
  1: () => ({ request: apiVersionsRequestV1({}), response: apiVersionsResponseV1, logResponseError: false }),
  2: () => ({ request: apiVersionsRequestV2({}), response: apiVersionsResponseV2, logResponseError: false }),
  3: (options) => ({
    request: apiVersionsRequestV3(options),
    response: apiVersionsResponseV3,
    logResponseError: false,
  }),
};

export const ApiVersions: RequestFamily<ApiVersionsRequestOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no ApiVersions protocol for version ${version}`);
    return factory;
  },
});
