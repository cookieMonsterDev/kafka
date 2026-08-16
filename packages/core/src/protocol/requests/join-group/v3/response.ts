import type { ResponseDefinition } from '../../../schema';
import { joinGroupResponseV2 } from '../v2/response';

type JoinGroupResponseV2Body = Awaited<ReturnType<typeof joinGroupResponseV2.decode>>;
export type JoinGroupResponseV3Body = Omit<JoinGroupResponseV2Body, 'throttleTime'> & {
  throttleTime: number;
  clientSideThrottleTime: number;
};

/**
 * Starting in version 3, on quota violation, brokers send the response before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v2; only the meaning of `throttleTime` changes.
 */
export const joinGroupResponseV3: ResponseDefinition<JoinGroupResponseV3Body> = {
  decode: async (rawData) => {
    const decoded = await joinGroupResponseV2.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await joinGroupResponseV2.parse(data);
    return data;
  },
};
