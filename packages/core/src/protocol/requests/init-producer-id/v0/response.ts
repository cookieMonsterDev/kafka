import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes.js';
import { defineResponse, field, int16, int64, int32, object } from '../../../schema.js';

/**
 * InitProducerId Response (Version: 0) => throttle_time_ms error_code producer_id producer_epoch
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   producer_id => INT64
 *   producer_epoch => INT16
 */
const bodySchema = object([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('producerId', int64),
  field('producerEpoch', int16),
]);

export const initProducerIdResponseV0 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    failIfVersionNotSupported(data.errorCode);
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
});
