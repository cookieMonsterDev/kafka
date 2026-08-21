import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes';
import { array, bytes, defineResponse, field, int16, int32, int64, object, string } from '../../../schema';

export interface DescribeDelegationTokenRenewer {
  principalType: string;
  name: string;
}

export interface DescribedDelegationToken {
  principalType: string;
  principalName: string;
  issueTimestamp: bigint;
  expiryTimestamp: bigint;
  maxTimestamp: bigint;
  tokenId: string;
  hmac: Buffer;
  renewers: DescribeDelegationTokenRenewer[];
}

export interface DescribeDelegationTokenResponseV0Body {
  errorCode: number;
  tokens: DescribedDelegationToken[];
  throttleTime: number;
}

/**
 * DescribeDelegationToken Response (Version: 0) => error_code [token_details] throttle_time_ms
 *   token_details => principal_type principal_name issue_timestamp expiry_timestamp max_timestamp
 *     token_id hmac [renewers]
 *   renewers => principal_type principal_name
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const renewerSchema = object([field('principalType', string), field('name', string)]);
const tokenSchema = object([
  field('principalType', string),
  field('principalName', string),
  field('issueTimestamp', int64),
  field('expiryTimestamp', int64),
  field('maxTimestamp', int64),
  field('tokenId', string),
  field('hmac', bytes),
  field('renewers', array(renewerSchema)),
]);
export const responseSchema = object([
  field('errorCode', int16),
  field('tokens', array(tokenSchema)),
  field('throttleTime', int32),
]);

export const describeDelegationTokenResponseV0 = defineResponse({
  schema: responseSchema,
  parse: async (data) => {
    failIfVersionNotSupported(data.errorCode);
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
});
