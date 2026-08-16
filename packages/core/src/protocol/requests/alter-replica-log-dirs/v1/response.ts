import type { ResponseDefinition } from '../../../schema';
import { alterReplicaLogDirsResponseV0, type AlterReplicaLogDirsResponseV0Body } from '../v0/response';

export interface AlterReplicaLogDirsResponseV1Body extends AlterReplicaLogDirsResponseV0Body {
  clientSideThrottleTime: number;
}

/**
 * Starting in version 1, on quota violation, brokers send out responses before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v0; only the meaning of `throttleTime` changes.
 */
export const alterReplicaLogDirsResponseV1: ResponseDefinition<AlterReplicaLogDirsResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = await alterReplicaLogDirsResponseV0.decode(rawData);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await alterReplicaLogDirsResponseV0.parse(data);
    return data;
  },
};
