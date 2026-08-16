import { defineRequest, field, nullableArray, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

const requestSchema = object([field('topics', nullableArray(string))]);

export const metadataRequestV2 = defineRequest({
  apiKey: API_KEYS.Metadata,
  apiVersion: 2,
  apiName: 'Metadata',
  schema: requestSchema,
});
