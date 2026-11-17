import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { COORDINATOR_TYPES } from '../protocol/enums/coordinator-types';
import { createTransactionsApi } from './transactions';
import { withSend } from '../broker/test-send';
import { REPLICA_UNAVAILABLE_ERRORS, STALE_COORDINATOR_ERRORS } from '../cluster/error-classifier';
import { coordinatorTarget, leaderTarget, nodeTarget } from '../cluster/run';
import { fakeRunCluster } from '../cluster/test-run';
import type { RetryOptions } from '../retry/index';

const logger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function makeApi(cluster: object, retry?: RetryOptions) {
  return createTransactionsApi({ cluster: cluster as unknown as Cluster, logger, rootLogger: logger, retry });
}

describe('admin/transactions', () => {
  it('groups transactional IDs by transaction coordinator', async () => {
    const firstCoordinator = withSend({
      nodeId: 1,
      describeTransactions: vi.fn(async ({ transactionalIds }: { transactionalIds: string[] }) => ({
        transactionStates: transactionalIds.map((transactionalId) => ({ transactionalId })),
      })),
    });
    const secondCoordinator = withSend({
      nodeId: 2,
      describeTransactions: vi.fn(async ({ transactionalIds }: { transactionalIds: string[] }) => ({
        transactionStates: transactionalIds.map((transactionalId) => ({ transactionalId })),
      })),
    });
    const cluster = fakeRunCluster({
      coordinator: (key) => (key === 'tx-c' ? secondCoordinator : firstCoordinator),
    });

    const result = await makeApi(cluster).describeTransactions(['tx-a', 'tx-b', 'tx-c']);

    expect(cluster.resolveTarget).toHaveBeenCalledTimes(3);
    expect(cluster.resolveTarget).toHaveBeenCalledWith(coordinatorTarget('tx-a', COORDINATOR_TYPES.TRANSACTION));
    expect(cluster.resolveTarget).toHaveBeenCalledWith(coordinatorTarget('tx-b', COORDINATOR_TYPES.TRANSACTION));
    expect(cluster.resolveTarget).toHaveBeenCalledWith(coordinatorTarget('tx-c', COORDINATOR_TYPES.TRANSACTION));
    expect(firstCoordinator.describeTransactions).toHaveBeenCalledWith({ transactionalIds: ['tx-a', 'tx-b'] });
    expect(secondCoordinator.describeTransactions).toHaveBeenCalledWith({ transactionalIds: ['tx-c'] });
    expect(result.transactionStates.map(({ transactionalId }) => transactionalId)).toEqual(['tx-a', 'tx-b', 'tx-c']);
  });

  it('returns immediately for an empty list', async () => {
    const cluster = fakeRunCluster({});

    await expect(makeApi(cluster).describeTransactions([])).resolves.toEqual({ transactionStates: [] });
    expect(cluster.run).not.toHaveBeenCalled();
  });

  it('rejects invalid transactional IDs', async () => {
    const cluster = {} as Cluster;
    const api = createTransactionsApi({ cluster, logger, rootLogger: logger });

    await expect(api.describeTransactions([''])).rejects.toThrow('Transactional IDs must be non-empty strings');
    await expect(api.describeTransactions(null as never)).rejects.toThrow('Invalid transactionalIds array');
  });

  it('fans listTransactions out to every broker and unique-merges by transactionalId', async () => {
    const firstBroker = withSend({
      listTransactions: vi.fn(async () => ({
        transactionStates: [
          { transactionalId: 'tx-a', producerId: 1n, transactionState: 'Ongoing' },
          { transactionalId: 'tx-shared', producerId: 3n, transactionState: 'Empty' },
        ],
      })),
    });
    const secondBroker = withSend({
      listTransactions: vi.fn(async () => ({
        transactionStates: [
          { transactionalId: 'tx-b', producerId: 2n, transactionState: 'Ongoing' },
          { transactionalId: 'tx-shared', producerId: 3n, transactionState: 'Empty' },
        ],
      })),
    });
    const cluster = fakeRunCluster(
      { node: (nodeId) => (nodeId === '1' ? firstBroker : secondBroker) },
      { getNodeIds: vi.fn(() => ['1', '2']) },
    );

    const result = await makeApi(cluster).listTransactions({ stateFilters: ['Ongoing'], producerIdFilters: [1n] });

    expect(cluster.refreshMetadata).toHaveBeenCalled();
    expect(cluster.resolveTarget).toHaveBeenCalledWith(nodeTarget('1'));
    expect(cluster.resolveTarget).toHaveBeenCalledWith(nodeTarget('2'));
    expect(cluster.run).toHaveBeenCalledWith(
      expect.any(Function),
      expect.any(Function),
      expect.objectContaining({ refreshMetadata: true, retryOn: STALE_COORDINATOR_ERRORS }),
    );
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
    const broker = withSend({
      listTransactions: vi.fn(async () => ({ transactionStates: [] })),
    });
    const cluster = fakeRunCluster({ node: broker }, { getNodeIds: vi.fn(() => ['1']) });

    await expect(makeApi(cluster).listTransactions()).resolves.toEqual({ transactionStates: [] });
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
    const coordinator = withSend({
      initProducerId: vi.fn(async () => ({ producerId: 7n, producerEpoch: 0, errorCode: 0 })),
    });
    const cluster = fakeRunCluster({ coordinator });

    const result = await makeApi(cluster).fenceProducers({ transactionalIds: ['tx-a'] });

    expect(cluster.resolveTarget).toHaveBeenCalledWith(coordinatorTarget('tx-a', COORDINATOR_TYPES.TRANSACTION));
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
    const coordinator = withSend({
      initProducerId: vi
        .fn()
        .mockRejectedValueOnce(
          new KafkaProtocolError({ message: 'Concurrent txn', type: 'CONCURRENT_TRANSACTIONS', code: 51 }),
        )
        .mockResolvedValueOnce({ producerId: 7n, producerEpoch: 1, errorCode: 0 }),
    });
    const cluster = fakeRunCluster({ coordinator });
    const api = makeApi(cluster, { maxRetryTime: 1_000, initialRetryTime: 10, factor: 0.2, multiplier: 2, retries: 3 });

    const result = await api.fenceProducers({ transactionalIds: ['tx-a'] });

    expect(coordinator.initProducerId).toHaveBeenCalledTimes(2);
    expect(result.results[0]?.errorCode).toBe(0);
    expect(result.results[0]?.producerEpoch).toBe(1);
  });

  it('writes an abort marker to the partition leader', async () => {
    const leader = withSend({
      writeTxnMarkers: vi.fn(async () => ({ markers: [] })),
      describeProducers: vi.fn(),
    });
    const cluster = fakeRunCluster({ leader });
    const api = makeApi(cluster);

    await api.abortTransaction({
      topic: 'orders',
      partition: 0,
      producerId: 9n,
      producerEpoch: 2,
      coordinatorEpoch: 1,
    });

    expect(cluster.resolveTarget).toHaveBeenCalledWith(leaderTarget('orders', 0));
    expect(cluster.run).toHaveBeenCalledWith(
      leaderTarget('orders', 0),
      expect.any(Function),
      expect.objectContaining({ retryOn: REPLICA_UNAVAILABLE_ERRORS }),
    );
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

  it('retries abortTransaction on the new leader after NOT_LEADER_OR_FOLLOWER', async () => {
    const { KafkaProtocolError } = await import('../errors');
    const notLeader = new KafkaProtocolError({ message: 'Not leader', type: 'NOT_LEADER_OR_FOLLOWER', code: 6 });
    const staleLeader = withSend({ writeTxnMarkers: vi.fn().mockRejectedValue(notLeader) });
    const currentLeader = withSend({ writeTxnMarkers: vi.fn(async () => ({ markers: [] })) });
    const cluster = fakeRunCluster({ leader: vi.fn().mockReturnValueOnce(staleLeader).mockReturnValue(currentLeader) });

    await makeApi(cluster, { retries: 1, initialRetryTime: 1 }).abortTransaction({
      topic: 'orders',
      partition: 0,
      producerId: 9n,
      producerEpoch: 2,
      coordinatorEpoch: 1,
    });

    expect(staleLeader.writeTxnMarkers).toHaveBeenCalledTimes(1);
    expect(currentLeader.writeTxnMarkers).toHaveBeenCalledTimes(1);
    expect(cluster.recover).toHaveBeenCalledWith(notLeader);
  });

  it('resolves coordinatorEpoch from describeProducers when omitted', async () => {
    const leader = withSend({
      writeTxnMarkers: vi.fn(async () => ({ markers: [] })),
      describeProducers: vi.fn(async () => ({
        topics: [
          {
            topic: 'orders',
            partitions: [{ partition: 0, activeProducers: [{ producerId: 9n, coordinatorEpoch: 4 }] }],
          },
        ],
      })),
    });
    const cluster = fakeRunCluster({ leader });
    const api = makeApi(cluster);

    await api.abortTransaction({
      topic: 'orders',
      partition: 0,
      producerId: 9n,
      producerEpoch: 2,
    });

    expect(leader.describeProducers).toHaveBeenCalledWith({ topics: [{ topic: 'orders', partitions: [0] }] });
    expect(leader.writeTxnMarkers).toHaveBeenCalledWith(
      expect.objectContaining({
        markers: [expect.objectContaining({ coordinatorEpoch: 4 })],
      }),
    );
  });

  it('forceTerminateTransaction delegates to fenceProducers', async () => {
    const coordinator = withSend({
      initProducerId: vi.fn(async () => ({ producerId: 3n, producerEpoch: 0, errorCode: 0 })),
    });
    const cluster = fakeRunCluster({ coordinator });

    const result = await makeApi(cluster).forceTerminateTransaction({ transactionalId: 'tx-z' });

    expect(result).toEqual({
      transactionalId: 'tx-z',
      errorCode: 0,
      producerId: 3n,
      producerEpoch: 0,
    });
    expect(cluster.resolveTarget).toHaveBeenCalledWith(coordinatorTarget('tx-z', COORDINATOR_TYPES.TRANSACTION));
  });
});
