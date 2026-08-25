import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { field, flexibleObject, int16, int32 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface PushTelemetryResponseV0Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  errorCode: number;
}

/**
 * PushTelemetry Response (Version: 0) => throttle_time_ms error_code TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *
 * Flexible from v0 (KIP-714).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const responseSchema = flexibleObject([field('throttleTime', int32), field('errorCode', int16)]);

export const pushTelemetryResponseV0: ResponseDefinition<PushTelemetryResponseV0Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
