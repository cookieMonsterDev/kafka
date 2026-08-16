import { defineRequest, field, nullableArray, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * Metadata Request (Version: 1) => [topics]
 *   topics => STRING
 */
const requestSchema = object([field('topics', nullableArray(string))]);

export const metadataRequestV1 = defineRequest({
  apiKey: API_KEYS.Metadata,
  apiVersion: 1,
  apiName: 'Metadata',
  schema: requestSchema,
});
