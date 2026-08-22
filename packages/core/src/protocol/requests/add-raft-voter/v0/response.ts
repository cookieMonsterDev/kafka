import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { compactNullableString, field, flexibleObject, int16, int32 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface AddRaftVoterResponseV0Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  errorCode: number;
  errorMessage: string | null;
}

/**
 * AddRaftVoter Response (Version: 0) => throttle_time_ms error_code error_message TAG_BUFFER
 *
 * Flexible from v0. Quota timing follows KIP-219.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
]);

export const addRaftVoterResponseV0: ResponseDefinition<AddRaftVoterResponseV0Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
