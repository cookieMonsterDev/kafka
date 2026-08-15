import { defineRequest, field, object, string } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';

/**
 * SaslHandshake Request (Version: 0) => mechanism
 *   mechanism => STRING
 */
const requestSchema = object([field('mechanism', string)]);

export const saslHandshakeRequestV0 = defineRequest({
  apiKey: API_KEYS.SaslHandshake,
  apiVersion: 0,
  apiName: 'SaslHandshake',
  schema: requestSchema,
});
