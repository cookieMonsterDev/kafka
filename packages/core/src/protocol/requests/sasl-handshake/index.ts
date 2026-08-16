import type { ProtocolFactory, RequestFamily } from '../index';
import { saslHandshakeRequestV0 } from './v0/request';
import { saslHandshakeResponseV0 } from './v0/response';
import { saslHandshakeRequestV1 } from './v1/request';
import { saslHandshakeResponseV1 } from './v1/response';

export interface SaslHandshakeOptions {
  mechanism: string;
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
};

export const SaslHandshake: RequestFamily<SaslHandshakeOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no SaslHandshake protocol for version ${version}`);
    return factory;
  },
});
