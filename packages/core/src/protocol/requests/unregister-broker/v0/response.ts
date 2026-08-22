import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { compactNullableString, field, flexibleObject, int16, int32 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface UnregisterBrokerResponseV0Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  errorCode: number;
  errorMessage: string | null;
}

/**
 * UnregisterBroker Response (Version: 0) => throttle_time_ms error_code error_message TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   error_message => COMPACT_NULLABLE_STRING
 *
 * Flexible from v0. Quota timing follows KIP-219: the decoded throttle is exposed as
 * `clientSideThrottleTime`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
]);

export const unregisterBrokerResponseV0: ResponseDefinition<UnregisterBrokerResponseV0Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
