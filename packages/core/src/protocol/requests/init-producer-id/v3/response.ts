import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes';
import { defineResponse, field, flexibleObject, int16, int32, int64 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface InitProducerIdResponseV3Body {
  errorCode: number;
  producerId: bigint;
  producerEpoch: number;
  throttleTime: number;
  clientSideThrottleTime: number;
}

/**
 * InitProducerId Response (Version: 3) => throttle_time_ms error_code producer_id producer_epoch TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   producer_id => INT64
 *   producer_epoch => INT16
 *
 * Flexible (KIP-482). Quota timing follows v1 (KIP-219): the broker sends the response before
 * throttling, so the decoded throttle is exposed as `clientSideThrottleTime`.
 * Response header v1's trailing TAG_BUFFER is skipped by `Connection` before `decode()` runs.
 */
const bodySchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('producerId', int64),
  field('producerEpoch', int16),
]);

const rawResponse = defineResponse({ schema: bodySchema });

export const initProducerIdResponseV3: ResponseDefinition<InitProducerIdResponseV3Body> = {
  decode: async (rawData) => {
    const decoded = await rawResponse.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    failIfVersionNotSupported(data.errorCode);
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
