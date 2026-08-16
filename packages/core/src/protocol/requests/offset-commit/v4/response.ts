import type { ResponseDefinition } from '../../../schema';
import { offsetCommitResponseV3 } from '../v3/response';

export interface OffsetCommitResponseV4Body {
  responses: { topic: string; partitions: { partition: number; errorCode: number }[] }[];
  throttleTime: number;
  clientSideThrottleTime: number;
}

/**
 * Starting in version 4, on quota violation, brokers send the response before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v3; only the meaning of `throttleTime` changes.
 */
export const offsetCommitResponseV4: ResponseDefinition<OffsetCommitResponseV4Body> = {
  decode: async (rawData) => {
    const decoded = await offsetCommitResponseV3.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await offsetCommitResponseV3.parse(data);
    return data;
  },
};
