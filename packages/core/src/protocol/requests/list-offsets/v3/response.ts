import type { ResponseDefinition } from '../../../schema.js';
import { listOffsetsResponseV2 } from '../v2/response.js';

export interface ListOffsetsResponseV3Body {
  responses: {
    topic: string;
    partitions: { partition: number; errorCode: number; timestamp: bigint; offset: bigint }[];
  }[];
  throttleTime: number;
  clientSideThrottleTime: number;
}

/**
 * Starting in version 3, on quota violation, brokers send the response before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v2; only the meaning of `throttleTime` changes.
 */
export const listOffsetsResponseV3: ResponseDefinition<ListOffsetsResponseV3Body> = {
  decode: async (rawData) => {
    const decoded = await listOffsetsResponseV2.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await listOffsetsResponseV2.parse(data);
    return data;
  },
};
