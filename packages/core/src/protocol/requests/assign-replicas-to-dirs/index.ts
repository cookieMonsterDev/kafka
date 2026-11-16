import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { assignReplicasToDirsRequestV0, type AssignReplicasToDirsDirectory } from './v0/request';
import { assignReplicasToDirsResponseV0 } from './v0/response';

export type { AssignReplicasToDirsDirectory, AssignReplicasToDirsRequestV0Fields } from './v0/request';
export type { AssignReplicasToDirsResponseV0Body } from './v0/response';

export interface AssignReplicasToDirsOptions {
  brokerId: number;
  brokerEpoch?: bigint;
  directories: AssignReplicasToDirsDirectory[];
}

const DEFAULT_BROKER_EPOCH = -1n;

const VERSIONS = {
  0: (options) => ({
    request: assignReplicasToDirsRequestV0({
      brokerId: options.brokerId,
      brokerEpoch: options.brokerEpoch ?? DEFAULT_BROKER_EPOCH,
      directories: options.directories,
    }),
    response: assignReplicasToDirsResponseV0,
  }),
} satisfies VersionFactories<AssignReplicasToDirsOptions>;

export const AssignReplicasToDirs = defineRequestFamily(KAFKA_APIS.AssignReplicasToDirs, VERSIONS);
