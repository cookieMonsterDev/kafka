import type { ResponseDefinition } from '../../../schema';
import { addPartitionsToTxnResponseV0 } from '../v0/response';

export interface AddPartitionsToTxnResponseV1Body {
  errors: { topic: string; partitionErrors: { partition: number; errorCode: number }[] }[];
  throttleTime: number;
  clientSideThrottleTime: number;
}

/**
 * Starting in version 1, on quota violation, brokers send the response before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v0; only the meaning of `throttleTime` changes.
 */
export const addPartitionsToTxnResponseV1: ResponseDefinition<AddPartitionsToTxnResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = await addPartitionsToTxnResponseV0.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await addPartitionsToTxnResponseV0.parse(data);
    return data;
  },
};
