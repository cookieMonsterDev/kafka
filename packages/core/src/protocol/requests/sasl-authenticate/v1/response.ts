import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import type { ResponseDefinition } from '../../../schema';
import { saslAuthenticateResponseV0 } from '../v0/response';

export interface SaslAuthenticateResponseV1Body {
  errorCode: number;
  errorMessage: string | null;
  authBytes: Buffer;
  sessionLifetimeMs: bigint;
}

/**
 * SaslAuthenticate Response (Version: 1) => error_code error_message sasl_auth_bytes session_lifetime_ms
 *   error_code => INT16
 *   error_message => NULLABLE_STRING
 *   sasl_auth_bytes => BYTES
 *   session_lifetime_ms => INT64
 */
export const saslAuthenticateResponseV1: ResponseDefinition<SaslAuthenticateResponseV1Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const errorCode = decoder.readInt16();
    const errorMessage = decoder.readString();
    const authBytes = new Encoder().writeBytes(decoder.readBytes()).buffer;
    const sessionLifetimeMs = decoder.readInt64();

    return { errorCode, errorMessage, authBytes, sessionLifetimeMs };
  },
  parse: async (data) => {
    await saslAuthenticateResponseV0.parse(data);
    return data;
  },
};
