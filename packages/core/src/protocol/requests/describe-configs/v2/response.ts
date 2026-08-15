import type { ResponseDefinition } from '../../../schema.js';
import { describeConfigsResponseV1, type DescribeConfigsResponseV1Body } from '../v1/response.js';

export interface DescribeConfigsResponseV2Body extends DescribeConfigsResponseV1Body {
  clientSideThrottleTime: number;
}

/**
 * Starting in version 2, on quota violation, brokers send the response before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v1; only the meaning of `throttleTime` changes.
 */
export const describeConfigsResponseV2: ResponseDefinition<DescribeConfigsResponseV2Body> = {
  decode: async (rawData) => {
    const decoded = await describeConfigsResponseV1.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await describeConfigsResponseV1.parse(data);
    return data;
  },
};
