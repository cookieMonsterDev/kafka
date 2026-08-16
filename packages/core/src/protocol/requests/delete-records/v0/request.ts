import { array, defineRequest, field, int32, int64, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface DeleteRecordsPartition {
  partition: number;
  offset: bigint;
}

export interface DeleteRecordsTopic {
  topic: string;
  partitions: DeleteRecordsPartition[];
}

export interface DeleteRecordsRequestV0Fields {
  topics: DeleteRecordsTopic[];
  timeout: number;
}

/**
 * DeleteRecords Request (Version: 0) => [topics] timeout_ms
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition offset
 *       partition => INT32
 *       offset => INT64
 *   timeout => INT32
 */
const partitionSchema = object([field('partition', int32), field('offset', int64)]);
export const topicSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const requestSchema = object([field('topics', array(topicSchema)), field('timeout', int32)]);

export const deleteRecordsRequestV0 = defineRequest({
  apiKey: API_KEYS.DeleteRecords,
  apiVersion: 0,
  apiName: 'DeleteRecords',
  schema: requestSchema,
});
