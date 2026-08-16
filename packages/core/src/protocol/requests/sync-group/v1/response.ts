import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes';
import { bytes, defineResponse, field, int16, int32, object } from '../../../schema';

/**
 * SyncGroup Response (Version: 1) => throttle_time_ms error_code member_assignment
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   member_assignment => BYTES
 */
const bodySchema = object([field('throttleTime', int32), field('errorCode', int16), field('memberAssignment', bytes)]);

export const syncGroupResponseV1 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    failIfVersionNotSupported(data.errorCode);
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
});
