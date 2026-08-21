import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes';
import { defineResponse, field, int16, int32, int64, object } from '../../../schema';

export interface RenewDelegationTokenResponseV0Body {
  errorCode: number;
  expiryTimestampMs: bigint;
  throttleTime: number;
}

/**
 * RenewDelegationToken Response (Version: 0) => error_code expiry_timestamp throttle_time_ms
 *
 * Throttle time is the last field. Brokers throttle before sending until version 1 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const responseSchema = object([
  field('errorCode', int16),
  field('expiryTimestampMs', int64),
  field('throttleTime', int32),
]);

export const renewDelegationTokenResponseV0 = defineResponse({
  schema: responseSchema,
  parse: async (data) => {
    failIfVersionNotSupported(data.errorCode);
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
});
