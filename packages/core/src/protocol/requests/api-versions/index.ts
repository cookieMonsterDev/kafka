import type { ProtocolFactory, RequestFamily } from '../index.js'
import { apiVersionsRequestV0 } from './v0/request.js'
import { apiVersionsResponseV0 } from './v0/response.js'
import { apiVersionsRequestV1 } from './v1/request.js'
import { apiVersionsResponseV1 } from './v1/response.js'
import { apiVersionsRequestV2 } from './v2/request.js'
import { apiVersionsResponseV2 } from './v2/response.js'

const VERSIONS: Readonly<Record<number, ProtocolFactory>> = {
  0: () => ({ request: apiVersionsRequestV0({}), response: apiVersionsResponseV0, logResponseError: true }),
  1: () => ({ request: apiVersionsRequestV1({}), response: apiVersionsResponseV1, logResponseError: false }),
  2: () => ({ request: apiVersionsRequestV2({}), response: apiVersionsResponseV2, logResponseError: false }),
}

export const ApiVersions: RequestFamily = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version]
    if (!factory) throw new Error(`Invariant violated: no ApiVersions protocol for version ${version}`)
    return factory
  },
})
