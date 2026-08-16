import { describe, expect, it, vi } from 'vitest';
import type { Broker } from '../../broker/index';
import type { Cluster } from '../../cluster/index';
import { KafkaNonRetriableError } from '../../errors';
import { createLogger, LOG_LEVELS } from '../../loggers/index';
import { COORDINATOR_TYPES } from '../../protocol/enums/coordinator-types';
import { createEosManager } from './index';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

const mockInitProducerIdResponse = {
  producerId: 1000n,
  producerEpoch: 1,
  errorCode: 0,
  throttleTime: 0,
  clientSideThrottleTime: 0,
};

function fakeBroker() {
  return {
    initProducerId: vi.fn().mockResolvedValue(mockInitProducerIdResponse),
    addPartitionsToTxn: vi.fn().mockResolvedValue(undefined),
    endTxn: vi.fn().mockResolvedValue(undefined),
    addOffsetsToTxn: vi.fn().mockResolvedValue(undefined),
    txnOffsetCommit: vi.fn().mockResolvedValue(undefined),
  };
}

function fakeCluster(broker: ReturnType<typeof fakeBroker>) {
  return {
    refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
    findGroupCoordinator: vi.fn().mockResolvedValue(broker),
    findControllerBroker: vi.fn().mockResolvedValue(broker),
  };
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

    expect(eosManager.getProducerId()).toBe(-1n);
    expect(eosManager.getProducerEpoch()).toBe(0);
    expect(eosManager.getSequence(topic, 1)).toBe(0);
    expect(eosManager.isInitialized()).toBe(false);

    await eosManager.initProducerId();

    expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalled();
    expect(broker.initProducerId).toHaveBeenCalledWith({ transactionalId: null, transactionTimeout: 30_000 });

    expect(eosManager.getProducerId()).toBe(mockInitProducerIdResponse.producerId);
    expect(eosManager.getProducerEpoch()).toBe(mockInitProducerIdResponse.producerEpoch);
    expect(eosManager.isInitialized()).toBe(true);
  });

  it('gets and updates the sequence per topic-partition, rotating past the int32 max', async () => {
    const broker = fakeBroker();
    const cluster = fakeCluster(broker);
    const eosManager = createEosManager({ logger: silentLogger, cluster: cluster as unknown as Cluster });

    expect(eosManager.getSequence(topic, 1)).toBe(0);
    eosManager.updateSequence(topic, 1, 10); // No effect before initialization.
    expect(eosManager.getSequence(topic, 1)).toBe(0);

    await eosManager.initProducerId();

    expect(eosManager.getSequence(topic, 1)).toBe(0);
    eosManager.updateSequence(topic, 1, 5);
    eosManager.updateSequence(topic, 1, 10);
    expect(eosManager.getSequence(topic, 1)).toBe(15);

    expect(eosManager.getSequence(topic, 2)).toBe(0); // Different partition.
    expect(eosManager.getSequence('foobar', 1)).toBe(0); // Different topic.

    eosManager.updateSequence(topic, 3, 2 ** 31 - 1);
    expect(eosManager.getSequence(topic, 3)).toBe(2 ** 31 - 1); // Int32 max is a valid sequence.
    eosManager.updateSequence(topic, 3, 1);
    expect(eosManager.getSequence(topic, 3)).toBe(0); // Rotated only after exceeding int32 max.

    await eosManager.initProducerId();
    expect(eosManager.getSequence(topic, 1)).toBe(0); // Sequences reset by initProducerId.
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
      expect(broker.initProducerId).toHaveBeenCalledWith({ transactionalId, transactionTimeout: 30_000 });
      expect(eosManager.getProducerId()).toBe(mockInitProducerIdResponse.producerId);
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
  });

  it('acquires and releases a per-broker lock only once initialized', async () => {
    const broker = fakeBroker();
    const cluster = fakeCluster(broker);
    const eosManager = createEosManager({ logger: silentLogger, cluster: cluster as unknown as Cluster });
    const fakeTargetBroker = { nodeId: 7 } as unknown as Broker;

    await expect(eosManager.acquireBrokerLock(fakeTargetBroker)).resolves.toBeUndefined();
    await expect(eosManager.releaseBrokerLock(fakeTargetBroker)).resolves.toBeUndefined();

    await eosManager.initProducerId();
    await eosManager.acquireBrokerLock(fakeTargetBroker);
    await eosManager.releaseBrokerLock(fakeTargetBroker);
  });
});
