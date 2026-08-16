import type { ProtocolFactory, RequestFamily } from '../index';
import { saslAuthenticateRequestV0 } from './v0/request';
import { saslAuthenticateResponseV0 } from './v0/response';
import { saslAuthenticateRequestV1 } from './v1/request';
import { saslAuthenticateResponseV1 } from './v1/response';

export interface SaslAuthenticateOptions {
  authBytes: Buffer;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<SaslAuthenticateOptions>>> = {
  0: (options) => ({ request: saslAuthenticateRequestV0(options), response: saslAuthenticateResponseV0 }),
  1: (options) => ({ request: saslAuthenticateRequestV1(options), response: saslAuthenticateResponseV1 }),
};

export const SaslAuthenticate: RequestFamily<SaslAuthenticateOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no SaslAuthenticate protocol for version ${version}`);
    return factory;
  },
});
