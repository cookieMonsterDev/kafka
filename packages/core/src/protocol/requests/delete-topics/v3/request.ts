import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v1/request';

/**
 * DeleteTopics Request (Version: 3) => [topics] timeout
 *   topics => STRING
 *   timeout => INT32
 *
 * Wire format is identical to v1/v2. The bump may return TOPIC_DELETION_DISABLED.
 */
export const deleteTopicsRequestV3 = defineRequest({
  apiKey: API_KEYS.DeleteTopics,
  apiVersion: 3,
  apiName: 'DeleteTopics',
  schema: requestSchema,
});
