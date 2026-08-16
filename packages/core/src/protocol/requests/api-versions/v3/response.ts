import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes';
import {
  compactArray,
  defineResponse,
  field,
  flexibleObject,
  int16,
  int32,
  type ResponseDefinition,
} from '../../../schema';
import type { ApiVersionsResponseV2Body } from '../v2/response';

export type ApiVersionsResponseV3Body = ApiVersionsResponseV2Body;

const apiVersionEntrySchema = flexibleObject([
  field('apiKey', int16),
  field('minVersion', int16),
  field('maxVersion', int16),
]);

/**
 * ApiVersions Response (Version: 3) => error_code [api_keys] throttle_time_ms TAG_BUFFER
 *   error_code => INT16
 *   api_keys => api_key min_version max_version TAG_BUFFER
 *     api_key => INT16
 *     min_version => INT16
 *     max_version => INT16
 *   throttle_time_ms => INT32
 *
 * Compact arrays/strings plus tagged fields (KIP-482). Throttle semantics match v2 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const bodySchema = flexibleObject([
  field('errorCode', int16),
  field('apiVersions', compactArray(apiVersionEntrySchema)),
  field('throttleTime', int32),
]);

const raw = defineResponse({
  schema: bodySchema,
});

export const apiVersionsResponseV3: ResponseDefinition<ApiVersionsResponseV3Body> = {
  decode: async (rawData) => {
    const decoded = await raw.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    failIfVersionNotSupported(data.errorCode);
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
