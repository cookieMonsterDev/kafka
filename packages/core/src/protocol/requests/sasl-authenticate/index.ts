import { Encoder } from '../../encoder';
import type { ProtocolFactory, RequestFamily } from '../index';
import { saslAuthenticateRequestV0 } from './v0/request';
import { saslAuthenticateResponseV0 } from './v0/response';
import { saslAuthenticateRequestV1 } from './v1/request';
import { saslAuthenticateResponseV1, type SaslAuthenticateResponseV1Body } from './v1/response';
import { saslAuthenticateRequestV2 } from './v2/request';
import { saslAuthenticateResponseV2 } from './v2/response';

export interface SaslAuthenticateOptions {
  authBytes: Buffer;
}

/**
 * Mechanism `encode()` returns Kafka BYTES (INT32 length + payload). v0/v1 write that buffer as
 * the whole body. v2 compact-encodes the inner SASL payload, so strip a well-formed length prefix.
 */
function unwrapLengthPrefixed(buffer: Buffer): Buffer {
  if (buffer.length < 4) return buffer;
  const size = buffer.readInt32BE(0);
  if (size < 0 || size !== buffer.length - 4) return buffer;
  return buffer.subarray(4);
}

/** Re-wrap so SCRAM/PLAIN `decode()` can keep calling `Decoder.readBytes()`. */
function wrapLengthPrefixed(buffer: Buffer): Buffer {
  return new Encoder().writeBytes(buffer).buffer;
}

const saslAuthenticateResponseV2ForAuthenticator = {
  decode: async (rawData: Buffer) => {
    const data = await saslAuthenticateResponseV2.decode(rawData);
    return { ...data, authBytes: wrapLengthPrefixed(data.authBytes) };
  },
  parse: async (data: SaslAuthenticateResponseV1Body) => saslAuthenticateResponseV2.parse(data),
};

const VERSIONS: Readonly<Record<number, ProtocolFactory<SaslAuthenticateOptions>>> = {
  0: (options) => ({ request: saslAuthenticateRequestV0(options), response: saslAuthenticateResponseV0 }),
  1: (options) => ({ request: saslAuthenticateRequestV1(options), response: saslAuthenticateResponseV1 }),
  2: (options) => ({
    request: saslAuthenticateRequestV2({ authBytes: unwrapLengthPrefixed(options.authBytes) }),
    response: saslAuthenticateResponseV2ForAuthenticator,
  }),
};

export const SaslAuthenticate: RequestFamily<SaslAuthenticateOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no SaslAuthenticate protocol for version ${version}`);
    return factory;
  },
});
