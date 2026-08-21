import type { ResponseDefinition } from '../../../schema';
import { expireDelegationTokenResponseV0, type ExpireDelegationTokenResponseV0Body } from '../v0/response';

export interface ExpireDelegationTokenResponseV1Body extends ExpireDelegationTokenResponseV0Body {
  clientSideThrottleTime: number;
}

/**
 * On quota violation, brokers send out responses before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 */
export const expireDelegationTokenResponseV1: ResponseDefinition<ExpireDelegationTokenResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = await expireDelegationTokenResponseV0.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await expireDelegationTokenResponseV0.parse(data);
    return data;
  },
};
