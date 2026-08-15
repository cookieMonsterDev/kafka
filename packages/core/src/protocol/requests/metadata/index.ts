import type { ProtocolFactory, RequestFamily } from '../index.js'
import { metadataRequestV0 } from './v0/request.js'
import { metadataResponseV0 } from './v0/response.js'
import { metadataRequestV1 } from './v1/request.js'
import { metadataResponseV1 } from './v1/response.js'
import { metadataRequestV2 } from './v2/request.js'
import { metadataResponseV2 } from './v2/response.js'
import { metadataRequestV3 } from './v3/request.js'
import { metadataResponseV3 } from './v3/response.js'
import { metadataRequestV4 } from './v4/request.js'
import { metadataResponseV4 } from './v4/response.js'
import { metadataRequestV5 } from './v5/request.js'
import { metadataResponseV5 } from './v5/response.js'
import { metadataRequestV6 } from './v6/request.js'
import { metadataResponseV6 } from './v6/response.js'

export interface MetadataOptions {
  topics?: string[]
  allowAutoTopicCreation?: boolean
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<MetadataOptions>>> = {
  0: ({ topics = [] }) => ({ request: metadataRequestV0({ topics }), response: metadataResponseV0 }),
  1: ({ topics = [] }) => ({ request: metadataRequestV1({ topics }), response: metadataResponseV1 }),
  2: ({ topics = [] }) => ({ request: metadataRequestV2({ topics }), response: metadataResponseV2 }),
  3: ({ topics = [] }) => ({ request: metadataRequestV3({ topics }), response: metadataResponseV3 }),
  4: ({ topics = [], allowAutoTopicCreation = true }) => ({
    request: metadataRequestV4({ topics, allowAutoTopicCreation }),
    response: metadataResponseV4,
  }),
  5: ({ topics = [], allowAutoTopicCreation = true }) => ({
    request: metadataRequestV5({ topics, allowAutoTopicCreation }),
    response: metadataResponseV5,
  }),
  6: ({ topics = [], allowAutoTopicCreation = true }) => ({
    request: metadataRequestV6({ topics, allowAutoTopicCreation }),
    response: metadataResponseV6,
  }),
}

export const Metadata: RequestFamily<MetadataOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version]
    if (!factory) throw new Error(`Invariant violated: no Metadata protocol for version ${version}`)
    return factory
  },
})
