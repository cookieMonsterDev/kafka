import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { compactArray, compactNullableString, compactString, field, flexibleObject, int16 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface AlterUserScramCredentialsResult {
  user: string;
  errorCode: number;
  errorMessage: string | null;
}

export interface AlterUserScramCredentialsResponseV0Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  results: AlterUserScramCredentialsResult[];
}

/**
 * AlterUserScramCredentials Response (Version: 0) => throttle_time_ms [results] TAG_BUFFER
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const resultSchema = flexibleObject([
  field('user', compactString),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
]);
const resultsSchema = compactArray(resultSchema);

export const alterUserScramCredentialsResponseV0: ResponseDefinition<AlterUserScramCredentialsResponseV0Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const clientSideThrottleTime = decoder.readInt32();
    const results = resultsSchema.read(decoder);
    decoder.readTaggedFields();
    return { throttleTime: 0, clientSideThrottleTime, results };
  },
  parse: async (data) => {
    const withError = data.results.find((result) => failure(result.errorCode));
    if (withError) throw createErrorFromCode(withError.errorCode);
    return data;
  },
};
