import { boolean, defineRequest, field, nullableArray, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

const requestSchema = object([field('topics', nullableArray(string)), field('allowAutoTopicCreation', boolean)]);

export const metadataRequestV5 = defineRequest({
  apiKey: API_KEYS.Metadata,
  apiVersion: 5,
  apiName: 'Metadata',
  schema: requestSchema,
});
