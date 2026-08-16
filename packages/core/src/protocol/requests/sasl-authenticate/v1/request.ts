import { defineRequest, field, object, rawBytes } from '../../../schema';
import { API_KEYS } from '../../api-keys';

const requestSchema = object([field('authBytes', rawBytes)]);

export const saslAuthenticateRequestV1 = defineRequest({
  apiKey: API_KEYS.SaslAuthenticate,
  apiVersion: 1,
  apiName: 'SaslAuthenticate',
  schema: requestSchema,
});
