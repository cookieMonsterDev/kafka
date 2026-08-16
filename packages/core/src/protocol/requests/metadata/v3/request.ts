import { defineRequest, field, nullableArray, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

const requestSchema = object([field('topics', nullableArray(string))]);

export const metadataRequestV3 = defineRequest({
  apiKey: API_KEYS.Metadata,
  apiVersion: 3,
  apiName: 'Metadata',
  schema: requestSchema,
});
