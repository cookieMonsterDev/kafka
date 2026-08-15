import type { ResponseDefinition } from '../../../schema.js';
import { syncGroupResponseV1 } from '../v1/response.js';

type SyncGroupResponseV1Body = Awaited<ReturnType<typeof syncGroupResponseV1.decode>>;
export type SyncGroupResponseV2Body = Omit<SyncGroupResponseV1Body, 'throttleTime'> & {
  throttleTime: number;
  clientSideThrottleTime: number;
};

/**
 * In version 2, on quota violation, brokers send the response before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v1; only the meaning of `throttleTime` changes.
 */
export const syncGroupResponseV2: ResponseDefinition<SyncGroupResponseV2Body> = {
  decode: async (rawData) => {
    const decoded = await syncGroupResponseV1.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await syncGroupResponseV1.parse(data);
    return data;
  },
};
