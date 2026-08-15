import { array, defineResponse, field, int32, object } from '../../../schema.js';
import { checkOffsetCommitErrors } from '../shared.js';
import { responseSchema } from '../v2/response.js';

/**
 * OffsetCommit Response (Version: 3) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code
 *       partition => INT32
 *       error_code => INT16
 */
const bodySchema = object([field('throttleTime', int32), field('responses', array(responseSchema))]);

export const offsetCommitResponseV3 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    checkOffsetCommitErrors(data);
    return data;
  },
});
