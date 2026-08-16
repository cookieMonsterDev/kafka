import type { ResponseDefinition } from '../../../schema';
import { txnOffsetCommitResponseV0 } from '../v0/response';

export interface TxnOffsetCommitResponseV1Body {
  topics: { topic: string; partitions: { partition: number; errorCode: number }[] }[];
  throttleTime: number;
  clientSideThrottleTime: number;
}

/**
 * Starting in version 1, on quota violation, brokers send the response before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v0; only the meaning of `throttleTime` changes.
 */
export const txnOffsetCommitResponseV1: ResponseDefinition<TxnOffsetCommitResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = await txnOffsetCommitResponseV0.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await txnOffsetCommitResponseV0.parse(data);
    return data;
  },
};
