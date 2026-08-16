import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v1/request';

/**
 * DeleteTopics Request (Version: 0) => [topics] timeout
 *   topics => STRING
 *   timeout => INT32
 *
 * Wire format is identical to v1.
 */
export const deleteTopicsRequestV0 = defineRequest({
  apiKey: API_KEYS.DeleteTopics,
  apiVersion: 0,
  apiName: 'DeleteTopics',
  schema: requestSchema,
});
