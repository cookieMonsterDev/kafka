import { array, defineResponse, field, int16, int32, int64, nullableString, object, string } from '../../../schema';
import { checkOffsetFetchPartitionErrors } from '../shared';

/**
 * OffsetFetch Response (Version: 1) => [responses]
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition offset metadata error_code
 *       partition => INT32
 *       offset => INT64
 *       metadata => NULLABLE_STRING
 *       error_code => INT16
 */
export const partitionSchema = object([
  field('partition', int32),
  field('offset', int64),
  field('metadata', nullableString),
  field('errorCode', int16),
]);
export const responseSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const bodySchema = object([field('responses', array(responseSchema))]);

export const offsetFetchResponseV1 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    checkOffsetFetchPartitionErrors(data);
    return data;
  },
});
