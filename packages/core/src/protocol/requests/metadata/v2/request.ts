import { defineRequest, field, nullableArray, object, string } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';

const requestSchema = object([field('topics', nullableArray(string))]);

export const metadataRequestV2 = defineRequest({
  apiKey: API_KEYS.Metadata,
  apiVersion: 2,
  apiName: 'Metadata',
  schema: requestSchema,
});
