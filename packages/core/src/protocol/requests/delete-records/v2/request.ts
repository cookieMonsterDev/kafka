import { compactArray, compactString, defineRequest, field, flexibleObject, int32, int64 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * DeleteRecords Request (Version: 2) => [topics] timeout_ms TAG_BUFFER
 *   topics => name [partitions] TAG_BUFFER
 *     name => COMPACT_STRING
 *     partitions => partition_index offset TAG_BUFFER
 *       partition_index => INT32
 *       offset => INT64
 *   timeout_ms => INT32
 *
 * First flexible version (KIP-482). Same fields as v0/v1. Request header v2's trailing
 * TAG_BUFFER is written by `createRequest`, not here.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = flexibleObject([field('partition', int32), field('offset', int64)]);
const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(partitionSchema))]);
export const requestSchema = flexibleObject([field('topics', compactArray(topicSchema)), field('timeout', int32)]);

export const deleteRecordsRequestV2 = defineRequest({
  apiKey: API_KEYS.DeleteRecords,
  apiVersion: 2,
  apiName: 'DeleteRecords',
  schema: requestSchema,
});
