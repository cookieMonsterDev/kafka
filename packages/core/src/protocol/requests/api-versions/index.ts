import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { apiVersionsRequestV0 } from './v0/request';
import { apiVersionsResponseV0 } from './v0/response';
import { apiVersionsRequestV1 } from './v1/request';
import { apiVersionsResponseV1 } from './v1/response';
import { apiVersionsRequestV2 } from './v2/request';
import { apiVersionsResponseV2 } from './v2/response';
import { apiVersionsRequestV3, type ApiVersionsRequestOptions } from './v3/request';
import { apiVersionsResponseV3 } from './v3/response';
import { apiVersionsRequestV4 } from './v4/request';
import { apiVersionsResponseV4 } from './v4/response';
import { apiVersionsRequestV5 } from './v5/request';
import { apiVersionsResponseV5 } from './v5/response';

export type { ApiVersionsRequestOptions };

const VERSIONS = {
  0: () => ({ request: apiVersionsRequestV0({}), response: apiVersionsResponseV0, logResponseError: true }),
  1: () => ({ request: apiVersionsRequestV1({}), response: apiVersionsResponseV1, logResponseError: false }),
  2: () => ({ request: apiVersionsRequestV2({}), response: apiVersionsResponseV2, logResponseError: false }),
  3: (options) => ({
    request: apiVersionsRequestV3(options),
    response: apiVersionsResponseV3,
    logResponseError: false,
  }),
  4: (options) => ({
    request: apiVersionsRequestV4(options),
    response: apiVersionsResponseV4,
    logResponseError: false,
  }),
  5: (options) => ({
    request: apiVersionsRequestV5(options),
    response: apiVersionsResponseV5,
    logResponseError: false,
  }),
} satisfies VersionFactories<ApiVersionsRequestOptions>;

export const ApiVersions = defineRequestFamily(KAFKA_APIS.ApiVersions, VERSIONS);
