import { KafkaNonRetriableError } from '../errors';
import { CONTROLLER } from '../cluster/run';
import { retrier as createRetrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown } from './helpers';
import type { AddRaftVoterOptions, RaftVoterListener, RemoveRaftVoterOptions } from './types';
import { AddRaftVoter, RemoveRaftVoter } from '../protocol/requests/lazy-families';

export interface RaftVotersApi {
  addRaftVoter: (options: AddRaftVoterOptions) => Promise<void>;
  removeRaftVoter: (options: RemoveRaftVoterOptions) => Promise<void>;
}

function validateTimeoutMs(timeoutMs: unknown): void {
  if (
    timeoutMs != null &&
    (!Number.isInteger(timeoutMs) || (timeoutMs as number) < 0 || (timeoutMs as number) > 2_147_483_647)
  ) {
    throw new KafkaNonRetriableError(`Invalid timeoutMs ${formatUnknown(timeoutMs)}`);
  }
}

function validateVoterId(voterId: unknown): number {
  if (!Number.isInteger(voterId) || (voterId as number) < 0) {
    throw new KafkaNonRetriableError(`Invalid voterId ${formatUnknown(voterId)}`);
  }
  return voterId as number;
}

function validateDirectoryId(voterDirectoryId: unknown): Buffer {
  if (!(voterDirectoryId instanceof Buffer) || voterDirectoryId.length !== 16) {
    throw new KafkaNonRetriableError(`Invalid voterDirectoryId ${formatUnknown(voterDirectoryId)}`);
  }
  return voterDirectoryId;
}

function validateListeners(listeners: unknown): RaftVoterListener[] {
  if (!Array.isArray(listeners) || listeners.length === 0) {
    throw new KafkaNonRetriableError(`Invalid listeners array ${formatUnknown(listeners)}`);
  }

  return listeners.map((listener) => {
    if (typeof listener !== 'object' || listener == null) {
      throw new KafkaNonRetriableError(`Invalid listener ${formatUnknown(listener)}`);
    }
    const { name, host, port } = listener as RaftVoterListener;
    if (typeof name !== 'string' || name.length === 0) {
      throw new KafkaNonRetriableError(`Invalid listener name ${formatUnknown(name)}`);
    }
    if (typeof host !== 'string' || host.length === 0) {
      throw new KafkaNonRetriableError(`Invalid listener host ${formatUnknown(host)}`);
    }
    if (!Number.isInteger(port) || port < 0 || port > 65_535) {
      throw new KafkaNonRetriableError(`Invalid listener port ${formatUnknown(port)}`);
    }
    return { name, host, port };
  });
}

export function createRaftVotersApi({ cluster, retry }: AdminContext): RaftVotersApi {
  const retrier = createRetrier(retry);

  const addRaftVoter = async ({
    clusterId,
    timeoutMs,
    voterId,
    voterDirectoryId,
    listeners,
    ackWhenCommitted,
  }: AddRaftVoterOptions): Promise<void> => {
    validateTimeoutMs(timeoutMs);
    const normalizedVoterId = validateVoterId(voterId);
    const normalizedDirectoryId = validateDirectoryId(voterDirectoryId);
    const normalizedListeners = validateListeners(listeners);

    await cluster.run(
      CONTROLLER,
      async (broker) => {
        await broker.send(AddRaftVoter, {
          clusterId,
          timeoutMs,
          voterId: normalizedVoterId,
          voterDirectoryId: normalizedDirectoryId,
          listeners: normalizedListeners,
          ackWhenCommitted,
        });
      },
      { retrier, refreshMetadata: true, description: 'Could not add raft voter' },
    );
  };

  const removeRaftVoter = async ({ clusterId, voterId, voterDirectoryId }: RemoveRaftVoterOptions): Promise<void> => {
    const normalizedVoterId = validateVoterId(voterId);
    const normalizedDirectoryId = validateDirectoryId(voterDirectoryId);

    await cluster.run(
      CONTROLLER,
      async (broker) => {
        await broker.send(RemoveRaftVoter, {
          clusterId,
          voterId: normalizedVoterId,
          voterDirectoryId: normalizedDirectoryId,
        });
      },
      { retrier, refreshMetadata: true, description: 'Could not remove raft voter' },
    );
  };

  return { addRaftVoter, removeRaftVoter };
}
