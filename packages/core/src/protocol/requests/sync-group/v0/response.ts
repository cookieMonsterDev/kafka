import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes.js';
import { bytes, defineResponse, field, int16, object } from '../../../schema.js';

/**
 * SyncGroup Response (Version: 0) => error_code member_assignment
 *   error_code => INT16
 *   member_assignment => BYTES
 */
const bodySchema = object([field('errorCode', int16), field('memberAssignment', bytes)]);

export const syncGroupResponseV0 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    failIfVersionNotSupported(data.errorCode);
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
});
