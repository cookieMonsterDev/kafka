import { createErrorFromCode, failure } from '../../../error-codes.js';
import { array, defineResponse, field, int16, int32, object } from '../../../schema.js';
import { groupSchema } from '../v0/response.js';

/**
 * ListGroups Response (Version: 1) => throttle_time_ms error_code [groups]
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   groups => group_id protocol_type
 */
const bodySchema = object([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('groups', array(groupSchema)),
]);

export const listGroupsResponseV1 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
});
