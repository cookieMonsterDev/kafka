import type { ResponseDefinition } from '../../../schema';
import { alterConfigsResponseV0, type AlterConfigsResponseV0Body } from '../v0/response';

export interface AlterConfigsResponseV1Body extends AlterConfigsResponseV0Body {
  clientSideThrottleTime: number;
}

/**
 * Starting in version 1, on quota violation, brokers send out responses before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v0; only the meaning of `throttleTime` changes.
 */
export const alterConfigsResponseV1: ResponseDefinition<AlterConfigsResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = await alterConfigsResponseV0.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await alterConfigsResponseV0.parse(data);
    return data;
  },
};
