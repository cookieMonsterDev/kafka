import type { ResponseDefinition } from '../../../schema.js';
import { type CreateTopicsResponseV2Body, createTopicsResponseV2 } from '../v2/response.js';

export interface CreateTopicsResponseV3Body extends CreateTopicsResponseV2Body {
  clientSideThrottleTime: number;
}

/**
 * Starting in version 3, on quota violation, brokers send out responses before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v2; only the meaning of `throttleTime` changes.
 */
export const createTopicsResponseV3: ResponseDefinition<CreateTopicsResponseV3Body> = {
  decode: async (rawData) => {
    const decoded = await createTopicsResponseV2.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await createTopicsResponseV2.parse(data);
    return data;
  },
};
