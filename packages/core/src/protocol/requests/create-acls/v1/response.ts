import { Decoder } from '../../../decoder.js';
import { createErrorFromCode, failure } from '../../../error-codes.js';
import { array, field, int16, nullableString, object } from '../../../schema.js';
import type { ResponseDefinition } from '../../../schema.js';

/**
 * On quota violation, brokers send out responses before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * CreateAcls Response (Version: 1) => throttle_time_ms [creation_responses]
 *   throttle_time_ms => INT32
 *   creation_responses => error_code error_message
 *     error_code => INT16
 *     error_message => NULLABLE_STRING
 *
 * The wire's throttle_time_ms is client-side (KIP-219); the raw value is exposed as
 * `clientSideThrottleTime` and `throttleTime` is always 0.
 */
const creationResponseSchema = object([field('errorCode', int16), field('errorMessage', nullableString)]);
const restSchema = object([field('creationResponses', array(creationResponseSchema))]);

export interface CreateAclsResponseV1Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  creationResponses: { errorCode: number; errorMessage: string | null }[];
}

export const createAclsResponseV1: ResponseDefinition<CreateAclsResponseV1Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const clientSideThrottleTime = decoder.readInt32();
    const rest = restSchema.read(decoder);
    return { throttleTime: 0, clientSideThrottleTime, ...rest };
  },
  parse: async (data) => {
    const withError = data.creationResponses.find(({ errorCode }) => failure(errorCode));
    if (withError) throw createErrorFromCode(withError.errorCode);
    return data;
  },
};
