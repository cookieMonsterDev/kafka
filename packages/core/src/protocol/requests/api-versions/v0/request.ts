import { defineRequest, object } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * ApiVersionRequest => ApiKeys
 */
const requestSchema = object([]);

export const apiVersionsRequestV0 = defineRequest({
  apiKey: API_KEYS.ApiVersions,
  apiVersion: 0,
  apiName: 'ApiVersions',
  schema: requestSchema,
});
