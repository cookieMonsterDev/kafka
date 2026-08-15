import { defineRequest, field, object, rawBytes } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';

const requestSchema = object([field('authBytes', rawBytes)]);

export const saslAuthenticateRequestV1 = defineRequest({
  apiKey: API_KEYS.SaslAuthenticate,
  apiVersion: 1,
  apiName: 'SaslAuthenticate',
  schema: requestSchema,
});
