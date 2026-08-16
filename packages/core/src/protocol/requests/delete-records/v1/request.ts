import { array, defineRequest, field, int32, object } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { topicSchema } from '../v0/request';

/**
 * DeleteRecords Request (Version: 1) => [topics] timeout_ms
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition offset
 *       partition => INT32
 *       offset => INT64
 *   timeout => INT32
 */
const requestSchema = object([field('topics', array(topicSchema)), field('timeout', int32)]);

export const deleteRecordsRequestV1 = defineRequest({
  apiKey: API_KEYS.DeleteRecords,
  apiVersion: 1,
  apiName: 'DeleteRecords',
  schema: requestSchema,
});
