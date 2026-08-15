import { Decoder } from '../../../decoder.js';
import { Encoder } from '../../../encoder.js';
import { KafkaJSProtocolError } from '../../../../errors.js';
import { createErrorFromCode, ERROR_CODES, failure } from '../../../error-codes.js';
import type { ResponseDefinition } from '../../../schema.js';

const SASL_AUTHENTICATION_FAILED = 58;
const protocolAuthError = ERROR_CODES.find((e) => e.code === SASL_AUTHENTICATION_FAILED);

export interface SaslAuthenticateResponseV0Body {
  errorCode: number;
  errorMessage: string | null;
  authBytes: Buffer;
}

/**
 * SaslAuthenticate Response (Version: 0) => error_code error_message sasl_auth_bytes
 *   error_code => INT16
 *   error_message => NULLABLE_STRING
 *   sasl_auth_bytes => BYTES
 */
export const saslAuthenticateResponseV0: ResponseDefinition<SaslAuthenticateResponseV0Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const errorCode = decoder.readInt16();
    const errorMessage = decoder.readString();

    /**
     * Original SASL mechanism protocols expect a length-prefixed byte response, so the bytes read
     * off the wire (already length-prefixed once, per the protocol) are re-wrapped with a fresh
     * length prefix before being handed back — matches kafkajs's behavior exactly.
     */
    const authBytes = new Encoder().writeBytes(decoder.readBytes()).buffer;

    return { errorCode, errorMessage, authBytes };
  },
  parse: async (data) => {
    if (data.errorCode === SASL_AUTHENTICATION_FAILED && data.errorMessage && protocolAuthError) {
      throw new KafkaJSProtocolError({ ...protocolAuthError, message: data.errorMessage });
    }
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
