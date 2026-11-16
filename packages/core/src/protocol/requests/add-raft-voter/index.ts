import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { addRaftVoterRequestV0, type AddRaftVoterListener } from './v0/request';
import { addRaftVoterResponseV0 } from './v0/response';
import { addRaftVoterRequestV1 } from './v1/request';

export type { AddRaftVoterListener, AddRaftVoterRequestV0Fields } from './v0/request';
export type { AddRaftVoterRequestV1Fields } from './v1/request';
export type { AddRaftVoterResponseV0Body } from './v0/response';

export interface AddRaftVoterOptions {
  clusterId?: string | null;
  timeoutMs?: number;
  voterId: number;
  voterDirectoryId: Buffer;
  listeners: AddRaftVoterListener[];
  ackWhenCommitted?: boolean;
}

const DEFAULT_TIMEOUT_MS = 60_000;

const sharedFields = (options: AddRaftVoterOptions) => ({
  clusterId: options.clusterId ?? null,
  timeoutMs: options.timeoutMs ?? DEFAULT_TIMEOUT_MS,
  voterId: options.voterId,
  voterDirectoryId: options.voterDirectoryId,
  listeners: options.listeners,
});

const VERSIONS = {
  0: (options) => ({
    request: addRaftVoterRequestV0(sharedFields(options)),
    response: addRaftVoterResponseV0,
  }),
  1: (options) => ({
    request: addRaftVoterRequestV1({
      ...sharedFields(options),
      ackWhenCommitted: options.ackWhenCommitted ?? true,
    }),
    response: addRaftVoterResponseV0,
  }),
} satisfies VersionFactories<AddRaftVoterOptions>;

export const AddRaftVoter = defineRequestFamily(KAFKA_APIS.AddRaftVoter, VERSIONS);
