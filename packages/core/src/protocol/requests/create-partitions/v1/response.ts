import type { ResponseDefinition } from '../../../schema';
import { type CreatePartitionsResponseV0Body, createPartitionsResponseV0 } from '../v0/response';

export interface CreatePartitionsResponseV1Body extends CreatePartitionsResponseV0Body {
  clientSideThrottleTime: number;
}

/**
 * Starting in version 1, on quota violation, brokers send out responses before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v0; only the meaning of `throttleTime` changes.
 */
export const createPartitionsResponseV1: ResponseDefinition<CreatePartitionsResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = await createPartitionsResponseV0.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await createPartitionsResponseV0.parse(data);
    return data;
  },
};
