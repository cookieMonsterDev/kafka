import type { ResponseDefinition } from '../../../schema.js';
import { addOffsetsToTxnResponseV0 } from '../v0/response.js';

export interface AddOffsetsToTxnResponseV1Body {
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
export const addOffsetsToTxnResponseV1: ResponseDefinition<AddOffsetsToTxnResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = await addOffsetsToTxnResponseV0.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await addOffsetsToTxnResponseV0.parse(data);
    return data;
  },
};
