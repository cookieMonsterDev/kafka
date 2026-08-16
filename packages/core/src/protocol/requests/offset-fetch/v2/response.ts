import { createErrorFromCode, failure } from '../../../error-codes';
import { array, defineResponse, field, int16, object } from '../../../schema';
import { checkOffsetFetchPartitionErrors } from '../shared';
import { responseSchema } from '../v1/response';

/**
 * OffsetFetch Response (Version: 2) => [responses] error_code
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition offset metadata error_code
 *       partition => INT32
 *       offset => INT64
 *       metadata => NULLABLE_STRING
 *       error_code => INT16
 *   error_code => INT16
 *
 * Adds a top-level error_code (e.g. for a coordinator-not-available failure that applies to the
 * whole request, distinct from a single partition's own error).
 */
const bodySchema = object([field('responses', array(responseSchema)), field('errorCode', int16)]);

export const offsetFetchResponseV2 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    checkOffsetFetchPartitionErrors(data);
    return data;
  },
});
