import { Decoder } from '../../../decoder';
import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes';
import { compactArray, compactBytes, compactString, field, flexibleObject, int16, int32, int64 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import type { DescribeDelegationTokenRenewer } from '../v0/response';
import type { DescribeDelegationTokenResponseV1Body } from '../v1/response';

export interface DescribedDelegationTokenV3 {
  principalType: string;
  principalName: string;
  tokenRequesterPrincipalType?: string;
  tokenRequesterPrincipalName?: string;
  issueTimestamp: bigint;
  expiryTimestamp: bigint;
  maxTimestamp: bigint;
  tokenId: string;
  hmac: Buffer;
  renewers: DescribeDelegationTokenRenewer[];
}

export interface DescribeDelegationTokenResponseV3Body extends Omit<DescribeDelegationTokenResponseV1Body, 'tokens'> {
  tokens: DescribedDelegationTokenV3[];
}

/**
 * DescribeDelegationToken Response (Version: 3) adds token requester details on each token.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const renewerSchema = flexibleObject([field('principalType', compactString), field('name', compactString)]);
const tokenSchema = flexibleObject([
  field('principalType', compactString),
  field('principalName', compactString),
  field('tokenRequesterPrincipalType', compactString),
  field('tokenRequesterPrincipalName', compactString),
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

export const describeDelegationTokenResponseV3: ResponseDefinition<DescribeDelegationTokenResponseV3Body> = {
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
