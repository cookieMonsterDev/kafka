import type { ResponseDefinition } from '../../../schema.js';
import { endTxnResponseV0 } from '../v0/response.js';

export interface EndTxnResponseV1Body {
  errorCode: number;
  throttleTime: number;
  clientSideThrottleTime: number;
}

/**
 * Starting in version 1, on quota violation, brokers send the response before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v0; only the meaning of `throttleTime` changes.
 */
export const endTxnResponseV1: ResponseDefinition<EndTxnResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = await endTxnResponseV0.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await endTxnResponseV0.parse(data);
    return data;
  },
};
