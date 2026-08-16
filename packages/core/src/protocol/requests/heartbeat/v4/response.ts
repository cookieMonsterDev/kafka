import { defineResponse, field, flexibleObject, int16, int32 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { heartbeatResponseV2, type HeartbeatResponseV2Body } from '../v2/response';

export type HeartbeatResponseV4Body = HeartbeatResponseV2Body;

const bodySchema = flexibleObject([field('throttleTime', int32), field('errorCode', int16)]);
const rawResponse = defineResponse({ schema: bodySchema });

/**
 * Heartbeat Response (Version: 4) => throttle_time_ms error_code TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *
 * First flexible version (KIP-482). Quota timing follows v2 (KIP-219): the broker sends the
 * response before throttling, so the decoded throttle is exposed as `clientSideThrottleTime`.
 * Response header v1's trailing TAG_BUFFER is skipped by `Connection` before `decode()` runs.
 */
export const heartbeatResponseV4: ResponseDefinition<HeartbeatResponseV4Body> = {
  decode: async (rawData) => {
    const decoded = await rawResponse.decode(rawData);
    return { errorCode: decoded.errorCode, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await heartbeatResponseV2.parse(data);
    return data;
  },
};
