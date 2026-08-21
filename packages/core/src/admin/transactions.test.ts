import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { COORDINATOR_TYPES } from '../protocol/enums/coordinator-types';
import { createTransactionsApi } from './transactions';

const logger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

describe('admin/transactions', () => {
  it('groups transactional IDs by transaction coordinator', async () => {
    const firstCoordinator = {
      nodeId: 1,
      describeTransactions: vi.fn(async ({ transactionalIds }: { transactionalIds: string[] }) => ({
        transactionStates: transactionalIds.map((transactionalId) => ({ transactionalId })),
      })),
    };
    const secondCoordinator = {
      nodeId: 2,
      describeTransactions: vi.fn(async ({ transactionalIds }: { transactionalIds: string[] }) => ({
        transactionStates: transactionalIds.map((transactionalId) => ({ transactionalId })),
      })),
    };
    const findGroupCoordinator = vi.fn(async ({ groupId }: { groupId: string }) =>
      groupId === 'tx-c' ? secondCoordinator : firstCoordinator,
    );
    const cluster = { findGroupCoordinator } as unknown as Cluster;
    const api = createTransactionsApi({ cluster, logger, rootLogger: logger });

    const result = await api.describeTransactions(['tx-a', 'tx-b', 'tx-c']);

    expect(findGroupCoordinator).toHaveBeenCalledTimes(3);
    expect(findGroupCoordinator).toHaveBeenCalledWith({
      groupId: 'tx-a',
      coordinatorType: COORDINATOR_TYPES.TRANSACTION,
    });
    expect(firstCoordinator.describeTransactions).toHaveBeenCalledWith({ transactionalIds: ['tx-a', 'tx-b'] });
    expect(secondCoordinator.describeTransactions).toHaveBeenCalledWith({ transactionalIds: ['tx-c'] });
    expect(result.transactionStates.map(({ transactionalId }) => transactionalId)).toEqual(['tx-a', 'tx-b', 'tx-c']);
  });

  it('returns immediately for an empty list', async () => {
    const findGroupCoordinator = vi.fn();
    const cluster = { findGroupCoordinator } as unknown as Cluster;
    const api = createTransactionsApi({ cluster, logger, rootLogger: logger });

    await expect(api.describeTransactions([])).resolves.toEqual({ transactionStates: [] });
    expect(findGroupCoordinator).not.toHaveBeenCalled();
  });

  it('rejects invalid transactional IDs', async () => {
    const cluster = {} as Cluster;
    const api = createTransactionsApi({ cluster, logger, rootLogger: logger });

    await expect(api.describeTransactions([''])).rejects.toThrow('Transactional IDs must be non-empty strings');
    await expect(api.describeTransactions(null as never)).rejects.toThrow('Invalid transactionalIds array');
  });
});
