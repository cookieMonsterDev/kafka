import type { ProtocolFactory, RequestFamily } from '../index';
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

const VERSIONS: Readonly<Record<number, ProtocolFactory<AddRaftVoterOptions>>> = {
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
};

export const AddRaftVoter: RequestFamily<AddRaftVoterOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no AddRaftVoter protocol for version ${version}`);
    return factory;
  },
});
