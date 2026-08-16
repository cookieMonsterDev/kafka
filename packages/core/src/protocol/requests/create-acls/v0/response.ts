import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { array, field, int16, nullableString, object } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

/**
 * CreateAcls Response (Version: 0) => throttle_time_ms [creation_responses]
 *   throttle_time_ms => INT32
 *   creation_responses => error_code error_message
 *     error_code => INT16
 *     error_message => NULLABLE_STRING
 */
const creationResponseSchema = object([field('errorCode', int16), field('errorMessage', nullableString)]);
const restSchema = object([field('creationResponses', array(creationResponseSchema))]);

export interface CreateAclsResponseV0Body {
  throttleTime: number;
  creationResponses: { errorCode: number; errorMessage: string | null }[];
}

export const createAclsResponseV0: ResponseDefinition<CreateAclsResponseV0Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const throttleTime = decoder.readInt32();
    const rest = restSchema.read(decoder);
    return { throttleTime, ...rest };
  },
  parse: async (data) => {
    const withError = data.creationResponses.find(({ errorCode }) => failure(errorCode));
    if (withError) throw createErrorFromCode(withError.errorCode);
    return data;
  },
};
