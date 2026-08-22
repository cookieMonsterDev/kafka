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

  it('fences producers through their transaction coordinators', async () => {
    const coordinator = {
      initProducerId: vi.fn(async () => ({ producerId: 7n, producerEpoch: 0, errorCode: 0 })),
    };
    const findGroupCoordinator = vi.fn(async () => coordinator);
    const cluster = { findGroupCoordinator } as unknown as Cluster;
    const api = createTransactionsApi({ cluster, logger, rootLogger: logger });

    const result = await api.fenceProducers({ transactionalIds: ['tx-a'] });

    expect(findGroupCoordinator).toHaveBeenCalled();
    expect(coordinator.initProducerId).toHaveBeenCalledWith({
      transactionalId: 'tx-a',
      transactionTimeout: 60_000,
      producerId: -1n,
      producerEpoch: -1,
    });
    expect(result.results[0]).toEqual({
      transactionalId: 'tx-a',
      errorCode: 0,
      producerId: 7n,
      producerEpoch: 0,
    });
  });

  it('retries fenceProducers when the coordinator returns CONCURRENT_TRANSACTIONS', async () => {
    const { KafkaProtocolError } = await import('../errors');
    const coordinator = {
      initProducerId: vi
        .fn()
        .mockRejectedValueOnce(
          new KafkaProtocolError({ message: 'Concurrent txn', type: 'CONCURRENT_TRANSACTIONS', code: 51 }),
        )
        .mockResolvedValueOnce({ producerId: 7n, producerEpoch: 1, errorCode: 0 }),
    };
    const findGroupCoordinator = vi.fn(async () => coordinator);
    const cluster = { findGroupCoordinator } as unknown as Cluster;
    const api = createTransactionsApi({
      cluster,
      logger,
      rootLogger: logger,
      retry: { maxRetryTime: 1_000, initialRetryTime: 10, factor: 0.2, multiplier: 2, retries: 3 },
    });

    const result = await api.fenceProducers({ transactionalIds: ['tx-a'] });

    expect(coordinator.initProducerId).toHaveBeenCalledTimes(2);
    expect(result.results[0]?.errorCode).toBe(0);
    expect(result.results[0]?.producerEpoch).toBe(1);
  });

  it('writes an abort marker to the partition leader', async () => {
    const leader = {
      writeTxnMarkers: vi.fn(async () => ({ markers: [] })),
      describeProducers: vi.fn(),
    };
    const cluster = {
      metadata: vi.fn(async () => ({ topics: [] })),
      findLeaderForPartitions: vi.fn(() => ({ '1': [0] })),
      findBroker: vi.fn(async () => leader),
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
    } as unknown as Cluster;
    const api = createTransactionsApi({ cluster, logger, rootLogger: logger });

    await api.abortTransaction({
      topic: 'orders',
      partition: 0,
      producerId: 9n,
      producerEpoch: 2,
      coordinatorEpoch: 1,
    });

    expect(leader.describeProducers).not.toHaveBeenCalled();
    expect(leader.writeTxnMarkers).toHaveBeenCalledWith({
      markers: [
        {
          producerId: 9n,
          producerEpoch: 2,
          transactionResult: false,
          coordinatorEpoch: 1,
          topics: [{ topic: 'orders', partitions: [0] }],
        },
      ],
    });
  });

  it('resolves coordinatorEpoch from describeProducers when omitted', async () => {
    const leader = {
      writeTxnMarkers: vi.fn(async () => ({ markers: [] })),
      describeProducers: vi.fn(async () => ({
        topics: [
          {
            topic: 'orders',
            partitions: [{ partition: 0, activeProducers: [{ producerId: 9n, coordinatorEpoch: 4 }] }],
          },
        ],
      })),
    };
    const cluster = {
      metadata: vi.fn(async () => ({ topics: [] })),
      findLeaderForPartitions: vi.fn(() => ({ '1': [0] })),
      findBroker: vi.fn(async () => leader),
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
    } as unknown as Cluster;
    const api = createTransactionsApi({ cluster, logger, rootLogger: logger });

    await api.abortTransaction({
      topic: 'orders',
      partition: 0,
      producerId: 9n,
      producerEpoch: 2,
    });

    expect(leader.describeProducers).toHaveBeenCalled();
    expect(leader.writeTxnMarkers).toHaveBeenCalledWith(
      expect.objectContaining({
        markers: [expect.objectContaining({ coordinatorEpoch: 4 })],
      }),
    );
  });

  it('forceTerminateTransaction delegates to fenceProducers', async () => {
    const coordinator = {
      initProducerId: vi.fn(async () => ({ producerId: 3n, producerEpoch: 0, errorCode: 0 })),
    };
    const cluster = {
      findGroupCoordinator: vi.fn(async () => coordinator),
    } as unknown as Cluster;
    const api = createTransactionsApi({ cluster, logger, rootLogger: logger });

    const result = await api.forceTerminateTransaction({ transactionalId: 'tx-z' });

    expect(result).toEqual({
      transactionalId: 'tx-z',
      errorCode: 0,
      producerId: 3n,
      producerEpoch: 0,
    });
  });
});
