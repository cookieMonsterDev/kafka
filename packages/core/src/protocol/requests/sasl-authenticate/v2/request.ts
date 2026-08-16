import { compactBytes, defineRequest, field, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * SaslAuthenticate Request (Version: 2) => auth_bytes TAG_BUFFER
 *   auth_bytes => COMPACT_BYTES
 *
 * First flexible version (KIP-482). Auth bytes are compact-encoded, not `rawBytes`. Request
 * header v2's trailing TAG_BUFFER is written by `createRequest`, not here.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([field('authBytes', compactBytes)]);

export const saslAuthenticateRequestV2 = defineRequest({
  apiKey: API_KEYS.SaslAuthenticate,
  apiVersion: 2,
  apiName: 'SaslAuthenticate',
  schema: requestSchema,
});
