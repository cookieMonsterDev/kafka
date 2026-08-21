import type { ResponseDefinition } from '../../../schema';
import { describeDelegationTokenResponseV0, type DescribeDelegationTokenResponseV0Body } from '../v0/response';

export interface DescribeDelegationTokenResponseV1Body extends DescribeDelegationTokenResponseV0Body {
  clientSideThrottleTime: number;
}

/**
 * On quota violation, brokers send out responses before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 */
export const describeDelegationTokenResponseV1: ResponseDefinition<DescribeDelegationTokenResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = await describeDelegationTokenResponseV0.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await describeDelegationTokenResponseV0.parse(data);
    return data;
  },
};
