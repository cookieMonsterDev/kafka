import { Decoder } from '../../../decoder';
import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes';
import type { ResponseDefinition } from '../../../schema';

export interface ApiVersionEntry {
  apiKey: number;
  minVersion: number;
  maxVersion: number;
}

export interface ApiVersionsResponseV1Body {
  errorCode: number;
  apiVersions: ApiVersionEntry[];
  throttleTime: number;
}

function readApiVersionEntry(decoder: Decoder): ApiVersionEntry {
  return {
    apiKey: decoder.readInt16(),
    minVersion: decoder.readInt16(),
    maxVersion: decoder.readInt16(),
  };
}

/**
 * ApiVersions Response (Version: 1) => error_code [api_versions] throttle_time_ms
 *   error_code => INT16
 *   api_versions => api_key min_version max_version
 *     api_key => INT16
 *     min_version => INT16
 *     max_version => INT16
 *   throttle_time_ms => INT32
 */
export const apiVersionsResponseV1: ResponseDefinition<ApiVersionsResponseV1Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const errorCode = decoder.readInt16();
    const apiVersions = decoder.readArray(readApiVersionEntry);

    /** Default to 0 when the broker omits the required throttle_time_ms field. */
    const throttleTime = decoder.canReadInt32() ? decoder.readInt32() : 0;

    return { errorCode, apiVersions, throttleTime };
  },
  parse: async (data) => {
    failIfVersionNotSupported(data.errorCode);
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
