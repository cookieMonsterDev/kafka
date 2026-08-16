import { defineRequest, object } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/** ApiVersions Request after v1 indicates the client can parse throttle_time_ms. */
const requestSchema = object([]);

export const apiVersionsRequestV1 = defineRequest({
  apiKey: API_KEYS.ApiVersions,
  apiVersion: 1,
  apiName: 'ApiVersions',
  schema: requestSchema,
});
