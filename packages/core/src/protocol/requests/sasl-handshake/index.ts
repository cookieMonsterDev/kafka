import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { saslHandshakeRequestV0 } from './v0/request';
import { saslHandshakeResponseV0 } from './v0/response';
import { saslHandshakeRequestV1 } from './v1/request';
import { saslHandshakeResponseV1 } from './v1/response';

export interface SaslHandshakeOptions {
  mechanism: string;
}

const VERSIONS = {
  0: ({ mechanism }) => ({
    request: saslHandshakeRequestV0({ mechanism }),
    response: saslHandshakeResponseV0,
  }),
  1: ({ mechanism }) => ({
    request: saslHandshakeRequestV1({ mechanism }),
    response: saslHandshakeResponseV1,
  }),
} satisfies VersionFactories<SaslHandshakeOptions>;

export const SaslHandshake = defineRequestFamily(KAFKA_APIS.SaslHandshake, VERSIONS);
