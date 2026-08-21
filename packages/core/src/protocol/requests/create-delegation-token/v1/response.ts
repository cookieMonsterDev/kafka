import type { ResponseDefinition } from '../../../schema';
import { createDelegationTokenResponseV0, type CreateDelegationTokenResponseV0Body } from '../v0/response';

export interface CreateDelegationTokenResponseV1Body extends CreateDelegationTokenResponseV0Body {
  clientSideThrottleTime: number;
}

/**
 * On quota violation, brokers send out responses before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v0; the raw throttle_time_ms is exposed as
 * `clientSideThrottleTime` and `throttleTime` is always 0.
 */
export const createDelegationTokenResponseV1: ResponseDefinition<CreateDelegationTokenResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = await createDelegationTokenResponseV0.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await createDelegationTokenResponseV0.parse(data);
    return data;
  },
};
