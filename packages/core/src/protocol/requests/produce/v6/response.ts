import type { ResponseDefinition } from '../../../schema';
import { produceResponseV5 } from '../v5/response';

export interface ProduceResponseV6Body {
  topics: {
    topicName: string;
    partitions: {
      partition: number;
      errorCode: number;
      baseOffset: bigint;
      logAppendTime: bigint;
      logStartOffset: bigint;
    }[];
  }[];
  throttleTime: number;
  clientSideThrottleTime: number;
}

/**
 * Starting in version 6, on quota violation, brokers send the response before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire shape is identical to v5; only the meaning of `throttleTime` changes.
 */
export const produceResponseV6: ResponseDefinition<ProduceResponseV6Body> = {
  decode: async (rawData) => {
    const decoded = await produceResponseV5.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await produceResponseV5.parse(data);
    return data;
  },
};
