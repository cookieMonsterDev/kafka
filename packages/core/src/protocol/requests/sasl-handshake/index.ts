import type { ProtocolFactory, RequestFamily } from '../index.js'
import { saslHandshakeRequestV0 } from './v0/request.js'
import { saslHandshakeResponseV0 } from './v0/response.js'
import { saslHandshakeRequestV1 } from './v1/request.js'
import { saslHandshakeResponseV1 } from './v1/response.js'

export interface SaslHandshakeOptions {
  mechanism: string
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<SaslHandshakeOptions>>> = {
  0: ({ mechanism }) => ({
    request: saslHandshakeRequestV0({ mechanism }),
    response: saslHandshakeResponseV0,
  }),
  1: ({ mechanism }) => ({
    request: saslHandshakeRequestV1({ mechanism }),
    response: saslHandshakeResponseV1,
  }),
}

export const SaslHandshake: RequestFamily<SaslHandshakeOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version]
    if (!factory) throw new Error(`Invariant violated: no SaslHandshake protocol for version ${version}`)
    return factory
  },
})
