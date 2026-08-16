import { array, defineRequest, field, int32, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface DeleteTopicsRequestV1Fields {
  topics: string[];
  timeout: number;
}

/**
 * DeleteTopics Request (Version: 1) => [topics] timeout
 *   topics => STRING
 *   timeout => INT32
 */
const requestSchema = object([field('topics', array(string)), field('timeout', int32)]);

export const deleteTopicsRequestV1 = defineRequest({
  apiKey: API_KEYS.DeleteTopics,
  apiVersion: 1,
  apiName: 'DeleteTopics',
  schema: requestSchema,
});
