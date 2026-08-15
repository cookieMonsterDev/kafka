import type { ProtocolFactory, RequestFamily } from '../index.js'
import { listGroupsRequestV0 } from './v0/request.js'
import { listGroupsResponseV0 } from './v0/response.js'
import { listGroupsRequestV1 } from './v1/request.js'
import { listGroupsResponseV1 } from './v1/response.js'
import { listGroupsRequestV2 } from './v2/request.js'
import { listGroupsResponseV2 } from './v2/response.js'

const VERSIONS: Readonly<Record<number, ProtocolFactory<Record<string, never>>>> = {
  0: () => ({ request: listGroupsRequestV0({}), response: listGroupsResponseV0 }),
  1: () => ({ request: listGroupsRequestV1({}), response: listGroupsResponseV1 }),
  2: () => ({ request: listGroupsRequestV2({}), response: listGroupsResponseV2 }),
}

export const ListGroups: RequestFamily<Record<string, never>> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version]
    if (!factory) throw new Error(`Invariant violated: no ListGroups protocol for version ${version}`)
    return factory
  },
})
