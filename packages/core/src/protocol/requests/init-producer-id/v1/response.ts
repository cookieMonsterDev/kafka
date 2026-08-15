import type { ResponseDefinition } from '../../../schema.js';
import { initProducerIdResponseV0 } from '../v0/response.js';

export interface InitProducerIdResponseV1Body {
  errorCode: number;
  producerId: bigint;
  producerEpoch: number;
  throttleTime: number;
  clientSideThrottleTime: number;
}

/**
 * Starting in version 1, on quota violation, brokers send the response before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v0; only the meaning of `throttleTime` changes.
 */
export const initProducerIdResponseV1: ResponseDefinition<InitProducerIdResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = await initProducerIdResponseV0.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await initProducerIdResponseV0.parse(data);
    return data;
  },
};
