import type { ResponseDefinition } from '../../../schema';
import { renewDelegationTokenResponseV0, type RenewDelegationTokenResponseV0Body } from '../v0/response';

export interface RenewDelegationTokenResponseV1Body extends RenewDelegationTokenResponseV0Body {
  clientSideThrottleTime: number;
}

/**
 * On quota violation, brokers send out responses before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 */
export const renewDelegationTokenResponseV1: ResponseDefinition<RenewDelegationTokenResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = await renewDelegationTokenResponseV0.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await renewDelegationTokenResponseV0.parse(data);
    return data;
  },
};
