import { defineRequest, field, object, rawBytes } from '../../../schema.js'
import { API_KEYS } from '../../api-keys.js'

/**
 * SaslAuthenticate Request (Version: 0) => sasl_auth_bytes
 *   sasl_auth_bytes => BYTES
 *
 * Despite the BNF, the body is the raw SASL mechanism bytes with no length prefix — matches
 * kafkajs's `Encoder#writeBuffer`, not `#writeBytes`.
 */
const requestSchema = object([field('authBytes', rawBytes)])

export const saslAuthenticateRequestV0 = defineRequest({
  apiKey: API_KEYS.SaslAuthenticate,
  apiVersion: 0,
  apiName: 'SaslAuthenticate',
  schema: requestSchema,
})
