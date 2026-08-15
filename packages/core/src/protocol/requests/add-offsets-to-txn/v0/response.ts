import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes.js';
import { defineResponse, field, int16, int32, object } from '../../../schema.js';

/**
 * AddOffsetsToTxn Response (Version: 0) => throttle_time_ms error_code
 *   throttle_time_ms => INT32
 *   error_code => INT16
 */
const bodySchema = object([field('throttleTime', int32), field('errorCode', int16)]);

export const addOffsetsToTxnResponseV0 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    failIfVersionNotSupported(data.errorCode);
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
});
