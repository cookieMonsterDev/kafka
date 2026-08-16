import { defineRequest, field, object, rawBytes } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * SaslAuthenticate Request (Version: 0) => sasl_auth_bytes
 *   sasl_auth_bytes => BYTES
 *
 * The body is the raw SASL mechanism bytes with no extra length prefix (`writeBuffer`).
 *
 * @see https://kafka.apache.org/43/security/authentication-using-sasl/
 */
const requestSchema = object([field('authBytes', rawBytes)]);

export const saslAuthenticateRequestV0 = defineRequest({
  apiKey: API_KEYS.SaslAuthenticate,
  apiVersion: 0,
  apiName: 'SaslAuthenticate',
  schema: requestSchema,
});
