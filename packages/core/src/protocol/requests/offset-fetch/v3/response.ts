import { createErrorFromCode, failure } from '../../../error-codes.js';
import { array, defineResponse, field, int16, int32, object } from '../../../schema.js';
import { checkOffsetFetchPartitionErrors } from '../shared.js';
import { responseSchema } from '../v1/response.js';

/**
 * OffsetFetch Response (Version: 3) => throttle_time_ms [responses] error_code
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition offset metadata error_code
 *       partition => INT32
 *       offset => INT64
 *       metadata => NULLABLE_STRING
 *       error_code => INT16
 *   error_code => INT16
 */
const bodySchema = object([
  field('throttleTime', int32),
  field('responses', array(responseSchema)),
  field('errorCode', int16),
]);

export const offsetFetchResponseV3 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    checkOffsetFetchPartitionErrors(data);
    return data;
  },
});
