import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { removeRaftVoterRequestV0 } from './v0/request';
import { removeRaftVoterResponseV0 } from './v0/response';

export type { RemoveRaftVoterRequestV0Fields } from './v0/request';
export type { RemoveRaftVoterResponseV0Body } from './v0/response';

export interface RemoveRaftVoterOptions {
  clusterId?: string | null;
  voterId: number;
  voterDirectoryId: Buffer;
}

const VERSIONS = {
  0: (options) => ({
    request: removeRaftVoterRequestV0({
      clusterId: options.clusterId ?? null,
      voterId: options.voterId,
      voterDirectoryId: options.voterDirectoryId,
    }),
    response: removeRaftVoterResponseV0,
  }),
} satisfies VersionFactories<RemoveRaftVoterOptions>;

export const RemoveRaftVoter = defineRequestFamily(KAFKA_APIS.RemoveRaftVoter, VERSIONS);
