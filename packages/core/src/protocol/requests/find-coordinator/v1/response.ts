import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes.js';
import { defineResponse, field, int16, int32, nullableString, object, string } from '../../../schema.js';

/**
 * FindCoordinator Response (Version: 1) => throttle_time_ms error_code error_message coordinator
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   error_message => NULLABLE_STRING
 *   coordinator => node_id host port
 *     node_id => INT32
 *     host => STRING
 *     port => INT32
 */
const coordinatorSchema = object([field('nodeId', int32), field('host', string), field('port', int32)]);
const bodySchema = object([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('errorMessage', nullableString),
  field('coordinator', coordinatorSchema),
]);

export const findCoordinatorResponseV1 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    failIfVersionNotSupported(data.errorCode);
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
});
