import type { ResponseDefinition } from '../../../schema';
import type { DeleteGroupsResult } from '../v0/response';
import { deleteGroupsResponseV0 } from '../v0/response';

export interface DeleteGroupsResponseV1Body {
  results: DeleteGroupsResult[];
  throttleTime: number;
  clientSideThrottleTime: number;
}

/**
 * Starting in version 1, on quota violation, brokers send the response before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v0; only the meaning of `throttleTime` changes.
 */
export const deleteGroupsResponseV1: ResponseDefinition<DeleteGroupsResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = await deleteGroupsResponseV0.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => data,
};
