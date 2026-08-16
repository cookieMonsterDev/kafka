import type { ResponseDefinition } from '../../../schema';
import { findCoordinatorResponseV1 } from '../v1/response';

export interface FindCoordinatorResponseV2Body {
  errorCode: number;
  errorMessage: string | null;
  coordinator: { nodeId: number; host: string; port: number };
  throttleTime: number;
  clientSideThrottleTime: number;
}

/**
 * Starting in version 2, on quota violation, brokers send the response before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v1; only the meaning of `throttleTime` changes.
 */
export const findCoordinatorResponseV2: ResponseDefinition<FindCoordinatorResponseV2Body> = {
  decode: async (rawData) => {
    const decoded = await findCoordinatorResponseV1.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await findCoordinatorResponseV1.parse(data);
    return data;
  },
};
