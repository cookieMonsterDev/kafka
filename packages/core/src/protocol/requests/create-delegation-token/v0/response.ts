import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes';
import { bytes, defineResponse, field, int16, int32, int64, object, string } from '../../../schema';

export interface CreateDelegationTokenResponseV0Body {
  errorCode: number;
  principalType: string;
  principalName: string;
  issueTimestampMs: bigint;
  expiryTimestampMs: bigint;
  maxTimestampMs: bigint;
  tokenId: string;
  hmac: Buffer;
  throttleTime: number;
}

/**
 * CreateDelegationToken Response (Version: 0) => error_code principal_type principal_name
 *   issue_timestamp expiry_timestamp max_timestamp token_id hmac throttle_time_ms
 *
 * Throttle time is the last field (unlike most admin APIs). Brokers throttle *before*
 * sending the response until version 1 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const responseSchema = object([
  field('errorCode', int16),
  field('principalType', string),
  field('principalName', string),
  field('issueTimestampMs', int64),
  field('expiryTimestampMs', int64),
  field('maxTimestampMs', int64),
  field('tokenId', string),
  field('hmac', bytes),
  field('throttleTime', int32),
]);

export const createDelegationTokenResponseV0 = defineResponse({
  schema: responseSchema,
  parse: async (data) => {
    failIfVersionNotSupported(data.errorCode);
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
});
