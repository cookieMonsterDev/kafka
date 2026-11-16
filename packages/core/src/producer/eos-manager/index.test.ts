import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../../cluster/index';
import { KafkaNonRetriableError, KafkaProtocolError } from '../../errors';
import { createLogger, LOG_LEVELS } from '../../loggers/index';
import { createErrorFromCode, ERROR_CODES } from '../../protocol/error-codes';
import { COORDINATOR_TYPES } from '../../protocol/enums/coordinator-types';
import { API_KEYS } from '../../protocol/requests/api-keys';
import { createEosManager } from './index';
import { withSend } from '../../broker/test-send';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

const mockInitProducerIdResponse = {
  producerId: 1000n,
  producerEpoch: 1,
  errorCode: 0,
  throttleTime: 0,
  clientSideThrottleTime: 0,
};

/** Second InitProducerId response, used to assert an epoch bump under transaction V2. */
const mockBumpedInitProducerIdResponse = {
  ...mockInitProducerIdResponse,
  producerEpoch: mockInitProducerIdResponse.producerEpoch + 1,
};

function fakeBroker() {
  return withSend({
    initProducerId: vi.fn().mockResolvedValue(mockInitProducerIdResponse),
    addPartitionsToTxn: vi.fn().mockResolvedValue(undefined),
    endTxn: vi.fn().mockResolvedValue(undefined),
    addOffsetsToTxn: vi.fn().mockResolvedValue(undefined),
    txnOffsetCommit: vi.fn().mockResolvedValue(undefined),
  });
}

/** `transactionV2` mirrors a broker advertising Produce v12 (Kafka 4.0+, KIP-890 part 2). */
function fakeCluster(broker: ReturnType<typeof fakeBroker>, { transactionV2 = false } = {}) {
  return withSend({
    refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
    findGroupCoordinator: vi.fn().mockResolvedValue(broker),
    findControllerBroker: vi.fn().mockResolvedValue(broker),
    brokerPool: { versions: transactionV2 ? { [API_KEYS.Produce]: { maxVersion: 12 } } : undefined },
  });
}

describe('producer/eosManager', () => {
  const topic = 'topic-name';

  it('initializes the producer id and epoch', async () => {
    const broker = fakeBroker();
    const cluster = fakeCluster(broker);
    const eosManager = createEosManager({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      transactionTimeout: 30_000,
    });

    expect(eosManager.ledger.getProducerId()).toBe(-1n);
    expect(eosManager.ledger.getProducerEpoch()).toBe(0);
    expect(eosManager.isInitialized()).toBe(false);

    await eosManager.initProducerId();

    expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalled();
    expect(broker.initProducerId).toHaveBeenCalledWith({
      transactionalId: null,
      transactionTimeout: 30_000,
      producerId: -1n,
      producerEpoch: -1,
    });

    expect(eosManager.ledger.getProducerId()).toBe(mockInitProducerIdResponse.producerId);
    expect(eosManager.ledger.getProducerEpoch()).toBe(mockInitProducerIdResponse.producerEpoch);
    expect(eosManager.isInitialized()).toBe(true);
  });

  it('passes the current producer id and epoch on later InitProducerId calls', async () => {
    const broker = fakeBroker();
    const cluster = fakeCluster(broker);
    const eosManager = createEosManager({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      transactionTimeout: 30_000,
    });

    await eosManager.initProducerId();
    expect(broker.initProducerId).toHaveBeenLastCalledWith({
      transactionalId: null,
      transactionTimeout: 30_000,
      producerId: -1n,
      producerEpoch: -1,
    });

    await eosManager.initProducerId();
    expect(broker.initProducerId).toHaveBeenLastCalledWith({
      transactionalId: null,
      transactionTimeout: 30_000,
      producerId: mockInitProducerIdResponse.producerId,
      producerEpoch: mockInitProducerIdResponse.producerEpoch,
    });
  });

  describe('when transactional', () => {
    const transactionalId = 'transactional-id';

    it('initializes the producer id and epoch via the transaction coordinator', async () => {
      const broker = fakeBroker();
      const cluster = fakeCluster(broker);
      const eosManager = createEosManager({
        logger: silentLogger,
        cluster: cluster as unknown as Cluster,
        transactionTimeout: 30_000,
        transactional: true,
        transactionalId,
      });

      await eosManager.initProducerId();

      expect(cluster.findGroupCoordinator).toHaveBeenCalledWith({
        groupId: transactionalId,
        coordinatorType: COORDINATOR_TYPES.TRANSACTION,
      });
      expect(broker.initProducerId).toHaveBeenCalledWith({
        transactionalId,
        transactionTimeout: 30_000,
        producerId: -1n,
        producerEpoch: -1,
      });
      expect(eosManager.ledger.getProducerId()).toBe(mockInitProducerIdResponse.producerId);
    });

    it('adds partitions to the transaction, skipping partitions already added', async () => {
      const broker = fakeBroker();
      const cluster = fakeCluster(broker);
      const eosManager = createEosManager({
        logger: silentLogger,
        cluster: cluster as unknown as Cluster,
        transactional: true,
        transactionalId,
      });

      await eosManager.initProducerId();
      eosManager.beginTransaction();

      const topicData = [
        { topic: 'test-1', partitions: [{ partition: 1 }, { partition: 2 }] },
        { topic: 'test-2', partitions: [{ partition: 1 }] },
      ];

      await eosManager.addPartitionsToTransaction(topicData);

      expect(cluster.findGroupCoordinator).toHaveBeenCalledWith({
        groupId: transactionalId,
        coordinatorType: COORDINATOR_TYPES.TRANSACTION,
      });
      expect(broker.addPartitionsToTxn).toHaveBeenCalledTimes(1);
      expect(broker.addPartitionsToTxn).toHaveBeenCalledWith({
        transactionalId,
        producerId: mockInitProducerIdResponse.producerId,
        producerEpoch: mockInitProducerIdResponse.producerEpoch,
        topics: [
          { topic: 'test-1', partitions: [1, 2] },
          { topic: 'test-2', partitions: [1] },
        ],
      });

      broker.addPartitionsToTxn.mockClear();
      await eosManager.addPartitionsToTransaction(topicData);
      expect(broker.addPartitionsToTxn).toHaveBeenCalledTimes(0); // No call if nothing new.

      broker.addPartitionsToTxn.mockClear();
      await eosManager.addPartitionsToTransaction([
        ...topicData,
        { topic: 'test-2', partitions: [{ partition: 2 }] },
        { topic: 'test-3', partitions: [{ partition: 1 }] },
      ]);
      expect(broker.addPartitionsToTxn).toHaveBeenCalledTimes(1); // Called if some are new.
      expect(broker.addPartitionsToTxn).toHaveBeenCalledWith(
        expect.objectContaining({
          topics: [
            { topic: 'test-2', partitions: [2] },
            { topic: 'test-3', partitions: [1] },
          ],
        }),
      );
    });

    it('skips AddPartitionsToTxn under transaction V2, tracking partitions locally instead', async () => {
      const broker = fakeBroker();
      const cluster = fakeCluster(broker, { transactionV2: true });
      const eosManager = createEosManager({
        logger: silentLogger,
        cluster: cluster as unknown as Cluster,
        transactional: true,
        transactionalId,
      });

      await eosManager.initProducerId();
      eosManager.beginTransaction();

      await eosManager.addPartitionsToTransaction([{ topic: 'test-topic-1', partitions: [{ partition: 0 }] }]);
      expect(broker.addPartitionsToTxn).not.toHaveBeenCalled();

      // Still tracked locally: commit sends EndTxn, proving the transaction is considered ongoing.
      await eosManager.commit();
      expect(broker.endTxn).toHaveBeenCalledWith(expect.objectContaining({ transactionResult: true }));
    });

    it('bumps the producer epoch after commit/abort under transaction V2', async () => {
      const broker = fakeBroker();
      broker.initProducerId
        .mockResolvedValueOnce(mockInitProducerIdResponse)
        .mockResolvedValueOnce(mockBumpedInitProducerIdResponse);
      const cluster = fakeCluster(broker, { transactionV2: true });
      const eosManager = createEosManager({
        logger: silentLogger,
        cluster: cluster as unknown as Cluster,
        transactional: true,
        transactionalId,
      });

      await eosManager.initProducerId();
      eosManager.beginTransaction();
      await eosManager.addPartitionsToTransaction([{ topic: 'test-topic-1', partitions: [{ partition: 0 }] }]);
      await eosManager.commit();

      expect(broker.initProducerId).toHaveBeenCalledTimes(2);
      expect(eosManager.ledger.getProducerEpoch()).toBe(mockBumpedInitProducerIdResponse.producerEpoch);
    });

    it('does not bump the producer epoch when the broker predates transaction V2', async () => {
      const broker = fakeBroker();
      const cluster = fakeCluster(broker, { transactionV2: false });
      const eosManager = createEosManager({
        logger: silentLogger,
        cluster: cluster as unknown as Cluster,
        transactional: true,
        transactionalId,
      });

      await eosManager.initProducerId();
      eosManager.beginTransaction();
      await eosManager.addPartitionsToTransaction([{ topic: 'test-topic-1', partitions: [{ partition: 0 }] }]);
      await eosManager.commit();

      expect(broker.initProducerId).toHaveBeenCalledTimes(1);
      expect(eosManager.ledger.getProducerEpoch()).toBe(mockInitProducerIdResponse.producerEpoch);
    });

    it('marks the transaction abort-only after TRANSACTION_ABORTABLE, rejecting commit but allowing abort', async () => {
      const broker = fakeBroker();
      const cluster = fakeCluster(broker);
      const eosManager = createEosManager({
        logger: silentLogger,
        cluster: cluster as unknown as Cluster,
        transactional: true,
        transactionalId,
      });

      await eosManager.initProducerId();
      eosManager.beginTransaction();
      await eosManager.addPartitionsToTransaction([{ topic: 'test-topic-1', partitions: [{ partition: 0 }] }]);

      eosManager.markTransactionAbortable();

      await expect(eosManager.commit()).rejects.toEqual(
        new KafkaNonRetriableError('Transaction state exception: Cannot call "commit" in state "ABORTABLE"'),
      );

      await eosManager.abort();
      expect(broker.endTxn).toHaveBeenCalledWith(expect.objectContaining({ transactionResult: false }));
    });

    it('is a no-op outside an active transaction', () => {
      const broker = fakeBroker();
      const cluster = fakeCluster(broker);
      const eosManager = createEosManager({
        logger: silentLogger,
        cluster: cluster as unknown as Cluster,
        transactional: true,
        transactionalId,
      });

      expect(() => eosManager.markTransactionAbortable()).not.toThrow();
    });

    it('commits a transaction', async () => {
      const broker = fakeBroker();
      const cluster = fakeCluster(broker);
      const eosManager = createEosManager({
        logger: silentLogger,
        cluster: cluster as unknown as Cluster,
        transactionTimeout: 30_000,
        transactional: true,
        transactionalId,
      });

      await expect(eosManager.commit()).rejects.toEqual(
        new KafkaNonRetriableError('Transaction state exception: Cannot call "commit" in state "UNINITIALIZED"'),
      );
      await eosManager.initProducerId();
      await expect(eosManager.commit()).rejects.toEqual(
        new KafkaNonRetriableError('Transaction state exception: Cannot call "commit" in state "READY"'),
      );

      eosManager.beginTransaction();
      await eosManager.addPartitionsToTransaction([{ topic: 'test-topic-1', partitions: [{ partition: 0 }] }]);
      await eosManager.commit();

      expect(broker.endTxn).toHaveBeenCalledWith({
        producerId: mockInitProducerIdResponse.producerId,
        producerEpoch: mockInitProducerIdResponse.producerEpoch,
        transactionalId,
        transactionResult: true,
      });
    });

    it('aborts a transaction', async () => {
      const broker = fakeBroker();
      const cluster = fakeCluster(broker);
      const eosManager = createEosManager({
        logger: silentLogger,
        cluster: cluster as unknown as Cluster,
        transactionTimeout: 30_000,
        transactional: true,
        transactionalId,
      });

      await eosManager.initProducerId();
      eosManager.beginTransaction();
      await eosManager.addPartitionsToTransaction([{ topic: 'test-topic-1', partitions: [{ partition: 0 }] }]);
      await eosManager.abort();

      expect(broker.endTxn).toHaveBeenCalledWith({
        producerId: mockInitProducerIdResponse.producerId,
        producerEpoch: mockInitProducerIdResponse.producerEpoch,
        transactionalId,
        transactionResult: false,
      });
    });

    it('retries EndTxn against a re-discovered coordinator on retriable errors', async () => {
      const broker = fakeBroker();
      broker.endTxn.mockRejectedValueOnce(new KafkaProtocolError(createErrorFromCode(16))).mockResolvedValue(undefined);
      const cluster = fakeCluster(broker);
      const eosManager = createEosManager({
        logger: silentLogger,
        cluster: cluster as unknown as Cluster,
        transactional: true,
        transactionalId,
        retry: { retries: 3, initialRetryTime: 1, maxRetryTime: 5 },
      });

      await eosManager.initProducerId();
      eosManager.beginTransaction();
      await eosManager.addPartitionsToTransaction([{ topic: 'test-topic-1', partitions: [{ partition: 0 }] }]);
      const lookupsBeforeCommit = cluster.findGroupCoordinator.mock.calls.length;
      await eosManager.commit();

      expect(broker.endTxn).toHaveBeenCalledTimes(2);
      expect(cluster.findGroupCoordinator.mock.calls.length - lookupsBeforeCommit).toBe(2);
      expect(eosManager.isInTransaction()).toBe(false);
    });

    it('lets commit be called again after EndTxn fails instead of sticking in COMMITTING', async () => {
      const broker = fakeBroker();
      broker.endTxn.mockRejectedValueOnce(new Error('connection lost')).mockResolvedValue(undefined);
      const cluster = fakeCluster(broker);
      const eosManager = createEosManager({
        logger: silentLogger,
        cluster: cluster as unknown as Cluster,
        transactional: true,
        transactionalId,
      });

      await eosManager.initProducerId();
      eosManager.beginTransaction();
      await eosManager.addPartitionsToTransaction([{ topic: 'test-topic-1', partitions: [{ partition: 0 }] }]);
      await expect(eosManager.commit()).rejects.toThrow('connection lost');
      await expect(eosManager.abort()).rejects.toThrow('Cannot call "abort" in state "COMMITTING"');

      await eosManager.commit();
      expect(broker.endTxn).toHaveBeenLastCalledWith(expect.objectContaining({ transactionResult: true }));
      eosManager.beginTransaction();
    });

    function errorOfType(type: string): KafkaProtocolError {
      return createErrorFromCode(ERROR_CODES.find((entry) => entry.type === type)!.code);
    }

    async function transactionWithPartition(broker: ReturnType<typeof fakeBroker>, { transactionV2 = false } = {}) {
      const eosManager = createEosManager({
        logger: silentLogger,
        cluster: fakeCluster(broker, { transactionV2 }) as unknown as Cluster,
        transactional: true,
        transactionalId,
      });
      await eosManager.initProducerId();
      eosManager.beginTransaction();
      await eosManager.addPartitionsToTransaction([{ topic: 'test-topic-1', partitions: [{ partition: 0 }] }]);
      return eosManager;
    }

    it.each(['PRODUCER_FENCED', 'INVALID_PRODUCER_EPOCH'])(
      'becomes unusable after EndTxn fails with %s instead of sticking in COMMITTING',
      async (type) => {
        const broker = fakeBroker();
        broker.endTxn.mockRejectedValue(errorOfType(type));
        const eosManager = await transactionWithPartition(broker);

        await expect(eosManager.commit()).rejects.toMatchObject({ type });

        const fatal = 'fenced by a newer instance';
        await expect(eosManager.commit()).rejects.toThrow(fatal);
        await expect(eosManager.abort()).rejects.toThrow(fatal);
        await expect(eosManager.initProducerId()).rejects.toThrow(fatal);
        expect(() => eosManager.beginTransaction()).toThrow(fatal);
        expect(broker.endTxn).toHaveBeenCalledTimes(1);
      },
    );

    it('becomes unusable when an abort is fenced', async () => {
      const broker = fakeBroker();
      broker.endTxn.mockRejectedValue(errorOfType('PRODUCER_FENCED'));
      const eosManager = await transactionWithPartition(broker);

      await expect(eosManager.abort()).rejects.toMatchObject({ type: 'PRODUCER_FENCED' });
      await expect(eosManager.abort()).rejects.toThrow('fenced by a newer instance');
    });

    it('lets a commit rejected with INVALID_TXN_STATE be aborted', async () => {
      const broker = fakeBroker();
      broker.endTxn.mockRejectedValueOnce(errorOfType('INVALID_TXN_STATE')).mockResolvedValue(undefined);
      const eosManager = await transactionWithPartition(broker);

      await expect(eosManager.commit()).rejects.toMatchObject({ type: 'INVALID_TXN_STATE' });
      await expect(eosManager.commit()).rejects.toThrow('Cannot call "commit" in state "ABORTABLE"');
      await eosManager.abort();

      expect(broker.endTxn).toHaveBeenLastCalledWith(expect.objectContaining({ transactionResult: false }));
      expect(() => eosManager.beginTransaction()).not.toThrow();
    });

    it('reports a committed transaction V2 as committed when the epoch bump after it fails, bumping later', async () => {
      const broker = fakeBroker();
      broker.initProducerId
        .mockResolvedValueOnce(mockInitProducerIdResponse)
        .mockRejectedValueOnce(new Error('coordinator down'))
        .mockResolvedValueOnce(mockBumpedInitProducerIdResponse);
      const eosManager = await transactionWithPartition(broker, { transactionV2: true });

      await expect(eosManager.commit()).resolves.toBeUndefined();
      expect(broker.endTxn).toHaveBeenCalledWith(expect.objectContaining({ transactionResult: true }));
      expect(eosManager.ledger.getProducerEpoch()).toBe(mockInitProducerIdResponse.producerEpoch);

      await eosManager.ledger.bumpEpochIfRequested();
      expect(eosManager.ledger.getProducerEpoch()).toBe(mockBumpedInitProducerIdResponse.producerEpoch);
    });

    it('does not send EndTxn when aborting/committing a transaction with no operations', async () => {
      const broker = fakeBroker();
      const cluster = fakeCluster(broker);
      const eosManager = createEosManager({
        logger: silentLogger,
        cluster: cluster as unknown as Cluster,
        transactional: true,
        transactionalId,
      });

      await eosManager.initProducerId();
      eosManager.beginTransaction();
      await expect(eosManager.abort()).resolves.toBeUndefined();
      expect(eosManager.isInTransaction()).toBe(false);
      expect(broker.endTxn).not.toHaveBeenCalled();

      eosManager.beginTransaction();
      await expect(eosManager.commit()).resolves.toBeUndefined();
      expect(broker.endTxn).not.toHaveBeenCalled();
    });

    it('sends offsets, marking them added to the transaction', async () => {
      const consumerGroupId = 'consumer-group-id';
      const topics = [{ topic: 'test-topic-1', partitions: [{ partition: 0, offset: 5n }] }];
      const broker = fakeBroker();
      const cluster = fakeCluster(broker);
      const eosManager = createEosManager({
        logger: silentLogger,
        cluster: cluster as unknown as Cluster,
        transactionTimeout: 30_000,
        transactional: true,
        transactionalId,
      });

      await eosManager.initProducerId();
      eosManager.beginTransaction();

      await eosManager.sendOffsets({ consumerGroupId, topics });

      expect(cluster.findGroupCoordinator).toHaveBeenCalledWith({
        groupId: consumerGroupId,
        coordinatorType: COORDINATOR_TYPES.GROUP,
      });
      expect(broker.addOffsetsToTxn).toHaveBeenCalledWith({
        transactionalId,
        producerId: mockInitProducerIdResponse.producerId,
        producerEpoch: mockInitProducerIdResponse.producerEpoch,
        groupId: consumerGroupId,
      });
      expect(broker.txnOffsetCommit).toHaveBeenCalledWith({
        transactionalId,
        producerId: mockInitProducerIdResponse.producerId,
        producerEpoch: mockInitProducerIdResponse.producerEpoch,
        groupId: consumerGroupId,
        topics: [{ topic: 'test-topic-1', partitions: [{ partition: 0, offset: 5n, metadata: null }] }],
      });

      await eosManager.commit();
      expect(broker.endTxn).toHaveBeenCalledWith(expect.objectContaining({ transactionResult: true }));
    });

    it('retries sendOffsets while the group coordinator is still loading', async () => {
      const consumerGroupId = 'consumer-group-id';
      const topics = [{ topic: 'test-topic-1', partitions: [{ partition: 0, offset: 5n }] }];
      const broker = fakeBroker();
      broker.txnOffsetCommit
        .mockRejectedValueOnce(Object.assign(new Error('loading'), { type: 'GROUP_LOAD_IN_PROGRESS' }))
        .mockResolvedValueOnce(undefined);
      const cluster = fakeCluster(broker);
      const eosManager = createEosManager({
        logger: silentLogger,
        cluster: cluster as unknown as Cluster,
        transactional: true,
        transactionalId,
      });

      await eosManager.initProducerId();
      eosManager.beginTransaction();

      await eosManager.sendOffsets({ consumerGroupId, topics });
      expect(broker.txnOffsetCommit).toHaveBeenCalledTimes(2);
    });

    it('looks up a fresh group coordinator and retries when the current one is stale', async () => {
      const consumerGroupId = 'consumer-group-id';
      const topics = [{ topic: 'test-topic-1', partitions: [{ partition: 0, offset: 5n }] }];
      const staleBroker = fakeBroker();
      staleBroker.txnOffsetCommit.mockRejectedValue(
        Object.assign(new Error('stale'), { type: 'NOT_COORDINATOR_FOR_GROUP' }),
      );
      const freshBroker = fakeBroker();
      const cluster = fakeCluster(staleBroker);
      cluster.findGroupCoordinator = vi
        .fn()
        .mockResolvedValueOnce(staleBroker) // transaction coordinator lookup, during initProducerId
        .mockResolvedValueOnce(staleBroker) // transaction coordinator lookup, during sendOffsets
        .mockResolvedValueOnce(staleBroker) // group coordinator lookup, first attempt
        .mockResolvedValueOnce(freshBroker); // group coordinator lookup, after refresh

      const eosManager = createEosManager({
        logger: silentLogger,
        cluster: cluster as unknown as Cluster,
        transactional: true,
        transactionalId,
      });

      await eosManager.initProducerId();
      eosManager.beginTransaction();

      await eosManager.sendOffsets({ consumerGroupId, topics });
      expect(staleBroker.txnOffsetCommit).toHaveBeenCalledTimes(1);
      expect(freshBroker.txnOffsetCommit).toHaveBeenCalledTimes(1);
    });
  });

  describe('when non-transactional', () => {
    it('throws synchronously when beginning a transaction', async () => {
      const broker = fakeBroker();
      const cluster = fakeCluster(broker);
      const eosManager = createEosManager({ logger: silentLogger, cluster: cluster as unknown as Cluster });
      await eosManager.initProducerId();

      expect(() => eosManager.beginTransaction()).toThrow(
        new KafkaNonRetriableError('Method unavailable if non-transactional'),
      );
    });

    for (const method of ['addPartitionsToTransaction', 'sendOffsets', 'commit', 'abort'] as const) {
      it(`rejects ${method} while uninitialized`, async () => {
        const broker = fakeBroker();
        const cluster = fakeCluster(broker);
        const eosManager = createEosManager({ logger: silentLogger, cluster: cluster as unknown as Cluster });

        await expect((eosManager[method] as () => Promise<unknown>)()).rejects.toEqual(
          new KafkaNonRetriableError(`Transaction state exception: Cannot call "${method}" in state "UNINITIALIZED"`),
        );
      });
    }

    it('markTransactionAbortable is a no-op', async () => {
      const broker = fakeBroker();
      const cluster = fakeCluster(broker);
      const eosManager = createEosManager({ logger: silentLogger, cluster: cluster as unknown as Cluster });

      expect(() => eosManager.markTransactionAbortable()).not.toThrow();
    });
  });

  describe('epoch bump requests', () => {
    it('defers a transactional bump until the transaction is aborted', async () => {
      const broker = fakeBroker();
      broker.initProducerId
        .mockResolvedValueOnce(mockInitProducerIdResponse)
        .mockResolvedValueOnce(mockBumpedInitProducerIdResponse);
      const eosManager = createEosManager({
        logger: silentLogger,
        cluster: fakeCluster(broker) as unknown as Cluster,
        transactional: true,
        transactionalId: 'transactional-id',
      });
      await eosManager.initProducerId();
      eosManager.beginTransaction();
      await eosManager.addPartitionsToTransaction([{ topic, partitions: [{ partition: 0 }] }]);

      eosManager.ledger.requestEpochBump();
      eosManager.markTransactionAbortable();
      await eosManager.ledger.bumpEpochIfRequested();
      expect(broker.initProducerId).toHaveBeenCalledTimes(1);

      await eosManager.abort();
      expect(broker.endTxn).toHaveBeenCalledWith(
        expect.objectContaining({ producerEpoch: mockInitProducerIdResponse.producerEpoch, transactionResult: false }),
      );
      expect(broker.initProducerId).toHaveBeenCalledTimes(2);
      expect(eosManager.ledger.getProducerEpoch()).toBe(mockBumpedInitProducerIdResponse.producerEpoch);
    });
  });
});
