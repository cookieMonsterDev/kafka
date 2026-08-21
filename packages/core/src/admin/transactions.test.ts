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

  it('fans listTransactions out to every broker and unique-merges by transactionalId', async () => {
    const firstBroker = {
      listTransactions: vi.fn(async () => ({
        transactionStates: [
          { transactionalId: 'tx-a', producerId: 1n, transactionState: 'Ongoing' },
          { transactionalId: 'tx-shared', producerId: 3n, transactionState: 'Empty' },
        ],
      })),
    };
    const secondBroker = {
      listTransactions: vi.fn(async () => ({
        transactionStates: [
          { transactionalId: 'tx-b', producerId: 2n, transactionState: 'Ongoing' },
          { transactionalId: 'tx-shared', producerId: 3n, transactionState: 'Empty' },
        ],
      })),
    };
    const refreshMetadata = vi.fn().mockResolvedValue(undefined);
    const cluster = {
      refreshMetadata,
      getNodeIds: vi.fn(() => ['1', '2']),
      findBroker: vi.fn(async ({ nodeId }: { nodeId: string }) => (nodeId === '1' ? firstBroker : secondBroker)),
    } as unknown as Cluster;
    const api = createTransactionsApi({ cluster, logger, rootLogger: logger });

    const result = await api.listTransactions({ stateFilters: ['Ongoing'], producerIdFilters: [1n] });

    expect(refreshMetadata).toHaveBeenCalled();
    expect(firstBroker.listTransactions).toHaveBeenCalledWith({
      stateFilters: ['Ongoing'],
      producerIdFilters: [1n],
    });
    expect(secondBroker.listTransactions).toHaveBeenCalledWith({
      stateFilters: ['Ongoing'],
      producerIdFilters: [1n],
    });
    expect(result.transactionStates.map(({ transactionalId }) => transactionalId)).toEqual([
      'tx-a',
      'tx-shared',
      'tx-b',
    ]);
  });

  it('queries all brokers when listTransactions filters are omitted', async () => {
    const broker = {
      listTransactions: vi.fn(async () => ({ transactionStates: [] })),
    };
    const cluster = {
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
      getNodeIds: vi.fn(() => ['1']),
      findBroker: vi.fn(async () => broker),
    } as unknown as Cluster;
    const api = createTransactionsApi({ cluster, logger, rootLogger: logger });

    await expect(api.listTransactions()).resolves.toEqual({ transactionStates: [] });
    expect(broker.listTransactions).toHaveBeenCalledWith({});
  });

  it('rejects invalid listTransactions filters', async () => {
    const cluster = {} as Cluster;
    const api = createTransactionsApi({ cluster, logger, rootLogger: logger });

    await expect(api.listTransactions({ stateFilters: 'Ongoing' as never })).rejects.toThrow(
      'Invalid stateFilters array',
    );
    await expect(api.listTransactions({ producerIdFilters: [1 as never] })).rejects.toThrow(
      'Producer IDs must be bigint',
    );
    await expect(api.listTransactions({ durationFilter: 1_000 as never })).rejects.toThrow('Invalid durationFilter');
  });
});
