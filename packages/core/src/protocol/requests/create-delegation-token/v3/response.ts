import { Decoder } from '../../../decoder';
import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes';
import { compactBytes, compactString, field, flexibleObject, int16, int32, int64 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import type { CreateDelegationTokenResponseV1Body } from '../v1/response';

export interface CreateDelegationTokenResponseV3Body extends CreateDelegationTokenResponseV1Body {
  tokenRequesterPrincipalType?: string;
  tokenRequesterPrincipalName?: string;
}

/**
 * CreateDelegationToken Response (Version: 3) => error_code principal_type principal_name
 *   token_requester_principal_type token_requester_principal_name issue_timestamp_ms
 *   expiry_timestamp_ms max_timestamp_ms token_id hmac throttle_time_ms TAG_BUFFER
 *
 * Version 3 adds the token requester principal. Quota timing follows v1 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const responseSchema = flexibleObject([
  field('errorCode', int16),
  field('principalType', compactString),
  field('principalName', compactString),
  field('tokenRequesterPrincipalType', compactString),
  field('tokenRequesterPrincipalName', compactString),
  field('issueTimestampMs', int64),
  field('expiryTimestampMs', int64),
  field('maxTimestampMs', int64),
  field('tokenId', compactString),
  field('hmac', compactBytes),
  field('throttleTime', int32),
]);

export const createDelegationTokenResponseV3: ResponseDefinition<CreateDelegationTokenResponseV3Body> = {
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
