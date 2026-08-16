import type { ResponseDefinition } from '../../../schema';
import { describeLogDirsResponseV0, type DescribeLogDirsResponseV0Body } from '../v0/response';

export interface DescribeLogDirsResponseV1Body extends DescribeLogDirsResponseV0Body {
  clientSideThrottleTime: number;
}

/**
 * Starting in version 1, on quota violation, brokers send out responses before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v0; only the meaning of `throttleTime` changes.
 */
export const describeLogDirsResponseV1: ResponseDefinition<DescribeLogDirsResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = await describeLogDirsResponseV0.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await describeLogDirsResponseV0.parse(data);
    return data;
  },
};
