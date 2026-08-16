import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { compactArray, compactNullableString, field, flexibleObject, int16, int32 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import type { CreateAclsResponseV1Body } from '../v1/response';

export type CreateAclsResponseV2Body = CreateAclsResponseV1Body;

const creationResponseSchema = flexibleObject([
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
]);
const bodySchema = flexibleObject([
  field('throttleTime', int32),
  field('creationResponses', compactArray(creationResponseSchema)),
]);

/**
 * CreateAcls Response (Version: 2) => throttle_time_ms [creation_responses] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   creation_responses => error_code error_message TAG_BUFFER
 *     error_code => INT16
 *     error_message => COMPACT_NULLABLE_STRING
 *
 * Flexible compact + tagged form of v1. Quota timing follows v1 (KIP-219).
 * Response header v1's trailing TAG_BUFFER is skipped by `Connection` before `decode()` runs.
 */
export const createAclsResponseV2: ResponseDefinition<CreateAclsResponseV2Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    const withError = data.creationResponses.find(({ errorCode }) => failure(errorCode));
    if (withError) throw createErrorFromCode(withError.errorCode);
    return data;
  },
};
