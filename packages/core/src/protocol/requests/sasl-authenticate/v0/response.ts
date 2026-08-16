import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { KafkaProtocolError } from '../../../../errors';
import { createErrorFromCode, ERROR_CODES, failure } from '../../../error-codes';
import type { ResponseDefinition } from '../../../schema';

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
     * SASL mechanisms expect a length-prefixed byte response, so the already-prefixed wire
     * bytes are re-wrapped before being handed back.
     */
    const authBytes = new Encoder().writeBytes(decoder.readBytes()).buffer;

    return { errorCode, errorMessage, authBytes };
  },
  parse: async (data) => {
    if (data.errorCode === SASL_AUTHENTICATION_FAILED && data.errorMessage && protocolAuthError) {
      throw new KafkaProtocolError({ ...protocolAuthError, message: data.errorMessage });
    }
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
