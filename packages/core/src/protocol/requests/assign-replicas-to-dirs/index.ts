import type { ProtocolFactory, RequestFamily } from '../index';
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

const VERSIONS: Readonly<Record<number, ProtocolFactory<AssignReplicasToDirsOptions>>> = {
  0: (options) => ({
    request: assignReplicasToDirsRequestV0({
      brokerId: options.brokerId,
      brokerEpoch: options.brokerEpoch ?? DEFAULT_BROKER_EPOCH,
      directories: options.directories,
    }),
    response: assignReplicasToDirsResponseV0,
  }),
};

export const AssignReplicasToDirs: RequestFamily<AssignReplicasToDirsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no AssignReplicasToDirs protocol for version ${version}`);
    return factory;
  },
});
