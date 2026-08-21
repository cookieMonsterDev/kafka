import { Decoder } from '../../../decoder';
import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes';
import { field, flexibleObject, int16, int32, int64 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import type { RenewDelegationTokenResponseV1Body } from '../v1/response';

export type RenewDelegationTokenResponseV2Body = RenewDelegationTokenResponseV1Body;

/**
 * RenewDelegationToken Response (Version: 2) => error_code expiry_timestamp_ms throttle_time_ms TAG_BUFFER
 *
 * Flexible compact + tagged form of v1. Quota timing follows v1 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const responseSchema = flexibleObject([
  field('errorCode', int16),
  field('expiryTimestampMs', int64),
  field('throttleTime', int32),
]);

export const renewDelegationTokenResponseV2: ResponseDefinition<RenewDelegationTokenResponseV2Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    failIfVersionNotSupported(data.errorCode);
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
