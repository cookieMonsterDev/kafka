import { Decoder } from '../../../decoder';
import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes';
import { compactBytes, compactString, field, flexibleObject, int16, int32, int64 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import type { CreateDelegationTokenResponseV1Body } from '../v1/response';

export type CreateDelegationTokenResponseV2Body = CreateDelegationTokenResponseV1Body;

/**
 * CreateDelegationToken Response (Version: 2) => error_code principal_type principal_name
 *   issue_timestamp_ms expiry_timestamp_ms max_timestamp_ms token_id hmac throttle_time_ms TAG_BUFFER
 *
 * Flexible compact + tagged form of v1. Quota timing follows v1 (KIP-219).
 * Response header v1's trailing TAG_BUFFER is skipped by `Connection` before `decode()` runs.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const responseSchema = flexibleObject([
  field('errorCode', int16),
  field('principalType', compactString),
  field('principalName', compactString),
  field('issueTimestampMs', int64),
  field('expiryTimestampMs', int64),
  field('maxTimestampMs', int64),
  field('tokenId', compactString),
  field('hmac', compactBytes),
  field('throttleTime', int32),
]);

export const createDelegationTokenResponseV2: ResponseDefinition<CreateDelegationTokenResponseV2Body> = {
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
