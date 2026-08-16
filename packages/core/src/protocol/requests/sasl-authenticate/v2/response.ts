import { Decoder } from '../../../decoder';
import { compactBytes, compactNullableString, field, flexibleObject, int16, int64 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { saslAuthenticateResponseV1, type SaslAuthenticateResponseV1Body } from '../v1/response';

export type SaslAuthenticateResponseV2Body = SaslAuthenticateResponseV1Body;

/**
 * SaslAuthenticate Response (Version: 2) => error_code error_message auth_bytes session_lifetime_ms TAG_BUFFER
 *   error_code => INT16
 *   error_message => COMPACT_NULLABLE_STRING
 *   auth_bytes => COMPACT_BYTES
 *   session_lifetime_ms => INT64
 *
 * First flexible version (KIP-482). `authBytes` is the inner SASL payload from compactBytes —
 * not re-wrapped with a Kafka BYTES length prefix. Mechanism decode that still expects
 * length-prefixed bytes needs the authenticator to wrap (or to consume the inner buffer).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const bodySchema = flexibleObject([
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('authBytes', compactBytes),
  field('sessionLifetimeMs', int64),
]);

export const saslAuthenticateResponseV2: ResponseDefinition<SaslAuthenticateResponseV2Body> = {
  decode: async (rawData) => bodySchema.read(new Decoder(rawData)),
  parse: async (data) => {
    await saslAuthenticateResponseV1.parse(data);
    return data;
  },
};
