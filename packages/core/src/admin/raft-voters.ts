import { KafkaNonRetriableError } from '../errors';
import { retrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown, protocolType } from './helpers';
import type { AddRaftVoterOptions, RaftVoterListener, RemoveRaftVoterOptions } from './types';

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

export function createRaftVotersApi({ cluster, logger, retry }: AdminContext): RaftVotersApi {
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

    await retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        await broker.addRaftVoter({
          clusterId,
          timeoutMs,
          voterId: normalizedVoterId,
          voterDirectoryId: normalizedDirectoryId,
          listeners: normalizedListeners,
          ackWhenCommitted,
        });
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not add raft voter', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          await cluster.refreshMetadata();
          throw error;
        }
        bail(error as Error);
        throw error;
      }
    });
  };

  const removeRaftVoter = async ({ clusterId, voterId, voterDirectoryId }: RemoveRaftVoterOptions): Promise<void> => {
    const normalizedVoterId = validateVoterId(voterId);
    const normalizedDirectoryId = validateDirectoryId(voterDirectoryId);

    await retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        await broker.removeRaftVoter({
          clusterId,
          voterId: normalizedVoterId,
          voterDirectoryId: normalizedDirectoryId,
        });
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not remove raft voter', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          await cluster.refreshMetadata();
          throw error;
        }
        bail(error as Error);
        throw error;
      }
    });
  };

  return { addRaftVoter, removeRaftVoter };
}
