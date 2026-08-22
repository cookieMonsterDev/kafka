import type { ProtocolFactory, RequestFamily } from '../index';
import { removeRaftVoterRequestV0 } from './v0/request';
import { removeRaftVoterResponseV0 } from './v0/response';

export type { RemoveRaftVoterRequestV0Fields } from './v0/request';
export type { RemoveRaftVoterResponseV0Body } from './v0/response';

export interface RemoveRaftVoterOptions {
  clusterId?: string | null;
  voterId: number;
  voterDirectoryId: Buffer;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<RemoveRaftVoterOptions>>> = {
  0: (options) => ({
    request: removeRaftVoterRequestV0({
      clusterId: options.clusterId ?? null,
      voterId: options.voterId,
      voterDirectoryId: options.voterDirectoryId,
    }),
    response: removeRaftVoterResponseV0,
  }),
};

export const RemoveRaftVoter: RequestFamily<RemoveRaftVoterOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no RemoveRaftVoter protocol for version ${version}`);
    return factory;
  },
});
