import { Decoder } from '../../../decoder';
import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes';
import { compactArray, compactBytes, compactString, field, flexibleObject, int16, int32, int64 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import type { DescribeDelegationTokenResponseV1Body } from '../v1/response';

export type DescribeDelegationTokenResponseV2Body = DescribeDelegationTokenResponseV1Body;

/**
 * DescribeDelegationToken Response (Version: 2) => error_code [tokens] throttle_time_ms TAG_BUFFER
 *
 * Flexible compact + tagged form of v1. Quota timing follows v1 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const renewerSchema = flexibleObject([field('principalType', compactString), field('name', compactString)]);
const tokenSchema = flexibleObject([
  field('principalType', compactString),
  field('principalName', compactString),
  field('issueTimestamp', int64),
  field('expiryTimestamp', int64),
  field('maxTimestamp', int64),
  field('tokenId', compactString),
  field('hmac', compactBytes),
  field('renewers', compactArray(renewerSchema)),
]);
export const responseSchema = flexibleObject([
  field('errorCode', int16),
  field('tokens', compactArray(tokenSchema)),
  field('throttleTime', int32),
]);

export const describeDelegationTokenResponseV2: ResponseDefinition<DescribeDelegationTokenResponseV2Body> = {
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
