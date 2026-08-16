import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import {
  compactArray,
  compactNullableString,
  compactString,
  field,
  flexibleObject,
  int16,
  int32,
  int8,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface DescribeUserScramCredentialsCredentialInfo {
  mechanism: number;
  iterations: number;
}

export interface DescribeUserScramCredentialsResult {
  user: string;
  errorCode: number;
  errorMessage: string | null;
  credentialInfos: DescribeUserScramCredentialsCredentialInfo[];
}

export interface DescribeUserScramCredentialsResponseV0Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  errorCode: number;
  errorMessage: string | null;
  results: DescribeUserScramCredentialsResult[];
}

/**
 * DescribeUserScramCredentials Response (Version: 0) => throttle_time_ms error_code error_message [results] TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const credentialSchema = flexibleObject([field('mechanism', int8), field('iterations', int32)]);
const resultSchema = flexibleObject([
  field('user', compactString),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('credentialInfos', compactArray(credentialSchema)),
]);
const restSchema = flexibleObject([
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('results', compactArray(resultSchema)),
]);

export const describeUserScramCredentialsResponseV0: ResponseDefinition<DescribeUserScramCredentialsResponseV0Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const clientSideThrottleTime = decoder.readInt32();
    const rest = restSchema.read(decoder);
    return { throttleTime: 0, clientSideThrottleTime, ...rest };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    const userWithError = data.results.find((result) => failure(result.errorCode));
    if (userWithError) throw createErrorFromCode(userWithError.errorCode);
    return data;
  },
};
