import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../../cluster/index';
import { KafkaNonRetriableError, KafkaProtocolError } from '../../errors';
import { createLogger, LOG_LEVELS } from '../../loggers/index';
import { createErrorFromCode, ERROR_CODES } from '../../protocol/error-codes';
import { API_KEYS } from '../../protocol/requests/api-keys';
import type { BrokerVersions } from '../../protocol/requests/index';
import { COORDINATOR_TYPES } from '../../protocol/enums/coordinator-types';
import { retrier, type RetryOptions } from '../../retry/index';
import { createTransactionSession, type TransactionSession } from './index';
import { ABORT_BEFORE_BEGIN_MESSAGE, ABORT_ONLY_MESSAGE } from './transaction-state-machine';
import { withSend } from '../../broker/test-send';
import { fakeRunCluster } from '../../cluster/test-run';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });
const transactionalId = 'transactional-id';
const topic = 'test-topic-1';

const mockInitProducerIdResponse = {
  producerId: 1000n,
  producerEpoch: 1,
  errorCode: 0,
  throttleTime: 0,
  clientSideThrottleTime: 0,
};

/** The finalized features of a cluster with transaction version 2 on. */
const TRANSACTION_V2_FEATURES: Readonly<Record<string, number>> = { 'transaction.version': 2 };

/** An EndTxn v5 response: the producer id and the epoch the coordinator bumped to. */
const mockBumpedEndTxnResponse = { errorCode: 0, producerId: 1000n, producerEpoch: 2 };

/** Second InitProducerId response, used to assert an epoch bump. */
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

/**
 * `transactionV2` mirrors a cluster that finalized `transaction.version` 2 (Kafka 4.0+, KIP-890
 * part 2); `txnOffsetCommitMaxVersion` is the highest TxnOffsetCommit version it advertises.
 */
function fakeCluster(
  broker: ReturnType<typeof fakeBroker>,
  { transactionV2 = false, txnOffsetCommitMaxVersion = 5 } = {},
) {
  // `metadata` is set to null to stand for a producer that has not loaded any metadata yet.
  const brokerPool: {
    finalizedFeatures: Readonly<Record<string, number>>;
    versions: BrokerVersions;
    metadata: object | null;
  } = {
    finalizedFeatures: transactionV2 ? TRANSACTION_V2_FEATURES : {},
    versions: { [API_KEYS.TxnOffsetCommit]: { minVersion: 0, maxVersion: txnOffsetCommitMaxVersion } },
    metadata: {},
  };
  const cluster = withSend({
    findGroupCoordinator: vi.fn().mockResolvedValue(broker),
    markOffsetAsCommitted: vi.fn(),
    addMultipleTargetTopics: vi.fn().mockResolvedValue(undefined),
    findTopicId: vi.fn<(topic: string) => Buffer | undefined>().mockReturnValue(undefined),
    brokerPool,
  });
  // `Cluster.run` resolves a coordinator target through the lookup EndTxn uses directly, read at
  // call time so a test can replace it.
  const runCluster = fakeRunCluster({
    coordinator: (groupId, coordinatorType) =>
      (cluster.findGroupCoordinator as (options: object) => Promise<object>)({ groupId, coordinatorType }),
  });
  return Object.assign(cluster, { run: runCluster.run, recover: runCluster.recover });
}

function fakeSender() {
  return {
    send: vi.fn().mockResolvedValue([]),
    sendBatch: vi.fn().mockResolvedValue([]),
    flush: vi.fn().mockResolvedValue(undefined),
  };
}

function createSession(cluster: ReturnType<typeof fakeCluster>, retry?: RetryOptions): TransactionSession {
  return createTransactionSession({
    logger: silentLogger,
    cluster: cluster as unknown as Cluster,
    transactionalId,
    transactionTimeout: 30_000,
    retry: retrier(retry),
  });
}

function errorOfType(type: string): KafkaProtocolError {
  return createErrorFromCode(ERROR_CODES.find((entry) => entry.type === type)!.code);
}

/** A begun transaction with one enrolled partition, so ending it sends EndTxn. */
async function transactionWithPartition(broker: ReturnType<typeof fakeBroker>, { transactionV2 = false } = {}) {
  const session = createSession(fakeCluster(broker, { transactionV2 }));
  const sender = fakeSender();
  const txn = await session.begin(() => sender);
  await session.enrollPartitions([{ topic, partitions: [{ partition: 0 }] }]);
  return { session, sender, txn };
}

describe('producer/transactionSession', () => {
  describe('begin', () => {
    it('initializes the producer id via the transaction coordinator on the first transaction only', async () => {
      const broker = fakeBroker();
      const cluster = fakeCluster(broker);
      const session = createSession(cluster);

      expect(session.ledger.isInitialized()).toBe(false);
      const first = await session.begin(fakeSender);

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
      expect(session.ledger.getProducerId()).toBe(mockInitProducerIdResponse.producerId);
      expect(first.isActive()).toBe(true);

      await first.commit();
      await session.begin(fakeSender);
      expect(broker.initProducerId).toHaveBeenCalledTimes(1);
    });

    it('rejects while another transaction is open', async () => {
      const session = createSession(fakeCluster(fakeBroker()));
      await session.begin(fakeSender);

      await expect(session.begin(fakeSender)).rejects.toEqual(
        new KafkaNonRetriableError(
          'There is already an ongoing transaction for this producer. Please end the transaction before beginning another.',
        ),
      );
    });

    it('rejects while the open transaction can only be aborted', async () => {
      const session = createSession(fakeCluster(fakeBroker()));
      await session.begin(fakeSender);
      session.sendFailed();

      await expect(session.begin(fakeSender)).rejects.toThrow(ABORT_BEFORE_BEGIN_MESSAGE);
    });
  });

  describe('enrollPartitions', () => {
    it('adds new partitions to the transaction, returning nothing to await once all are enrolled', async () => {
      const broker = fakeBroker();
      const cluster = fakeCluster(broker);
      const session = createSession(cluster);
      await session.begin(fakeSender);

      const topicData = [
        { topic: 'test-1', partitions: [{ partition: 1 }, { partition: 2 }] },
        { topic: 'test-2', partitions: [{ partition: 1 }] },
      ];

      await session.enrollPartitions(topicData);
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
      expect(session.enrollPartitions(topicData)).toBeUndefined();
      expect(broker.addPartitionsToTxn).not.toHaveBeenCalled();

      await session.enrollPartitions([
        ...topicData,
        { topic: 'test-2', partitions: [{ partition: 2 }] },
        { topic: 'test-3', partitions: [{ partition: 1 }] },
      ]);
      expect(broker.addPartitionsToTxn).toHaveBeenCalledTimes(1);
      expect(broker.addPartitionsToTxn).toHaveBeenCalledWith(
        expect.objectContaining({
          topics: [
            { topic: 'test-2', partitions: [2] },
            { topic: 'test-3', partitions: [1] },
          ],
        }),
      );
    });

    it('enrolls synchronously under transaction V2, without AddPartitionsToTxn', async () => {
      const broker = fakeBroker();
      const session = createSession(fakeCluster(broker, { transactionV2: true }));
      const txn = await session.begin(fakeSender);

      expect(session.enrollPartitions([{ topic, partitions: [{ partition: 0 }] }])).toBeUndefined();
      expect(broker.addPartitionsToTxn).not.toHaveBeenCalled();

      // Still enrolled: commit sends EndTxn, proving the transaction holds a partition.
      await txn.commit();
      expect(broker.endTxn).toHaveBeenCalledWith(expect.objectContaining({ transactionResult: true }));
    });

    it('rejects outside an open transaction', async () => {
      const session = createSession(fakeCluster(fakeBroker()));

      await expect(session.enrollPartitions([{ topic, partitions: [{ partition: 0 }] }])).rejects.toEqual(
        new KafkaNonRetriableError(
          'Transaction state exception: Cannot call "enrollPartitions" in state "UNINITIALIZED"',
        ),
      );
    });
  });

  describe('sendFailed', () => {
    it('leaves the transaction abort-only: sends and commit reject, abort works', async () => {
      const broker = fakeBroker();
      const { session, txn } = await transactionWithPartition(broker);

      session.sendFailed();

      expect(txn.isActive()).toBe(false);
      await expect(txn.send({ topic, messages: [] })).rejects.toThrow('can only be aborted');
      await expect(txn.commit()).rejects.toEqual(new KafkaNonRetriableError(ABORT_ONLY_MESSAGE));

      await txn.abort();
      expect(broker.endTxn).toHaveBeenCalledWith(expect.objectContaining({ transactionResult: false }));
    });

    it('is a no-op outside an open transaction', () => {
      const session = createSession(fakeCluster(fakeBroker()));
      expect(() => session.sendFailed()).not.toThrow();
    });
  });

  describe('ending a transaction', () => {
    it('flushes buffered sends before sending EndTxn', async () => {
      const broker = fakeBroker();
      const { sender, txn } = await transactionWithPartition(broker);

      await txn.commit();

      expect(sender.flush).toHaveBeenCalledTimes(1);
      expect(sender.flush.mock.invocationCallOrder[0]).toBeLessThan(broker.endTxn.mock.invocationCallOrder[0]!);
    });

    it('does not commit when a flushed send fails, leaving the transaction to abort', async () => {
      const broker = fakeBroker();
      const { session, sender, txn } = await transactionWithPartition(broker);
      sender.flush.mockImplementation(() => {
        session.sendFailed();
        return Promise.resolve();
      });

      await expect(txn.commit()).rejects.toThrow(ABORT_ONLY_MESSAGE);
      expect(broker.endTxn).not.toHaveBeenCalled();

      await txn.abort();
      expect(broker.endTxn).toHaveBeenCalledWith(expect.objectContaining({ transactionResult: false }));
    });

    it.each([
      ['commit', true],
      ['abort', false],
    ] as const)('%ss through EndTxn', async (call, transactionResult) => {
      const broker = fakeBroker();
      const { txn } = await transactionWithPartition(broker);

      await txn[call]();

      expect(broker.endTxn).toHaveBeenCalledWith({
        producerId: mockInitProducerIdResponse.producerId,
        producerEpoch: mockInitProducerIdResponse.producerEpoch,
        transactionalId,
        transactionResult,
        isTransactionV2Enabled: false,
      });
      expect(txn.isActive()).toBe(false);
    });

    it('does not send EndTxn for a transaction with no partitions or offsets', async () => {
      const broker = fakeBroker();
      const session = createSession(fakeCluster(broker));

      await expect((await session.begin(fakeSender)).abort()).resolves.toBeUndefined();
      await expect((await session.begin(fakeSender)).commit()).resolves.toBeUndefined();
      expect(broker.endTxn).not.toHaveBeenCalled();
    });

    it('rejects every call on a transaction once it ended, even after the next begins', async () => {
      const session = createSession(fakeCluster(fakeBroker()));
      const txn = await session.begin(fakeSender);
      await txn.abort();
      await session.begin(fakeSender);

      const ended = new KafkaNonRetriableError('Cannot continue to use transaction once ended');
      expect(txn.isActive()).toBe(false);
      await expect(txn.send({ topic, messages: [] })).rejects.toEqual(ended);
      await expect(txn.sendBatch({ topicMessages: [] })).rejects.toEqual(ended);
      await expect(txn.sendOffsets({ consumerGroupId: 'group', topics: [] })).rejects.toEqual(ended);
      await expect(txn.commit()).rejects.toEqual(ended);
      await expect(txn.abort()).rejects.toEqual(ended);
    });

    it('retries EndTxn against a re-discovered coordinator on retriable errors', async () => {
      const broker = fakeBroker();
      broker.endTxn.mockRejectedValueOnce(new KafkaProtocolError(createErrorFromCode(16))).mockResolvedValue(undefined);
      const cluster = fakeCluster(broker);
      const session = createSession(cluster, { retries: 3, initialRetryTime: 1, maxRetryTime: 5 });
      const txn = await session.begin(fakeSender);
      await session.enrollPartitions([{ topic, partitions: [{ partition: 0 }] }]);
      const lookupsBeforeCommit = cluster.findGroupCoordinator.mock.calls.length;

      await txn.commit();

      expect(broker.endTxn).toHaveBeenCalledTimes(2);
      expect(cluster.findGroupCoordinator.mock.calls.length - lookupsBeforeCommit).toBe(2);
    });

    it('lets commit be called again after EndTxn fails instead of sticking in COMMITTING', async () => {
      const broker = fakeBroker();
      broker.endTxn.mockRejectedValueOnce(new Error('connection lost')).mockResolvedValue(undefined);
      const { session, txn } = await transactionWithPartition(broker);

      await expect(txn.commit()).rejects.toThrow('connection lost');
      await expect(txn.send({ topic, messages: [] })).rejects.toThrow('can only be aborted');
      await expect(txn.abort()).rejects.toThrow('Cannot call "abort" in state "COMMITTING"');

      await txn.commit();
      expect(broker.endTxn).toHaveBeenLastCalledWith(expect.objectContaining({ transactionResult: true }));
      await expect(session.begin(fakeSender)).resolves.toBeTruthy();
    });

    it.each(['PRODUCER_FENCED', 'INVALID_PRODUCER_EPOCH'])(
      'makes the producer unusable after EndTxn fails with %s',
      async (type) => {
        const broker = fakeBroker();
        broker.endTxn.mockRejectedValue(errorOfType(type));
        const { session, txn } = await transactionWithPartition(broker);

        await expect(txn.commit()).rejects.toMatchObject({ type });

        const fenced = 'fenced by a newer instance';
        await expect(txn.send({ topic, messages: [] })).rejects.toThrow(fenced);
        await expect(txn.commit()).rejects.toThrow(fenced);
        await expect(txn.abort()).rejects.toThrow(fenced);
        await expect(session.begin(fakeSender)).rejects.toThrow(fenced);
        expect(broker.endTxn).toHaveBeenCalledTimes(1);
        expect(broker.initProducerId).toHaveBeenCalledTimes(1);
      },
    );

    it('makes the producer unusable when an abort is fenced', async () => {
      const broker = fakeBroker();
      broker.endTxn.mockRejectedValue(errorOfType('PRODUCER_FENCED'));
      const { txn } = await transactionWithPartition(broker);

      await expect(txn.abort()).rejects.toMatchObject({ type: 'PRODUCER_FENCED' });
      await expect(txn.abort()).rejects.toThrow('fenced by a newer instance');
    });

    it('lets a commit rejected with INVALID_TXN_STATE be aborted', async () => {
      const broker = fakeBroker();
      broker.endTxn.mockRejectedValueOnce(errorOfType('INVALID_TXN_STATE')).mockResolvedValue(undefined);
      const { session, txn } = await transactionWithPartition(broker);

      await expect(txn.commit()).rejects.toMatchObject({ type: 'INVALID_TXN_STATE' });
      await expect(txn.commit()).rejects.toThrow(ABORT_ONLY_MESSAGE);
      await txn.abort();

      expect(broker.endTxn).toHaveBeenLastCalledWith(expect.objectContaining({ transactionResult: false }));
      await expect(session.begin(fakeSender)).resolves.toBeTruthy();
    });

    it('lets a commit rejected with TRANSACTION_ABORTABLE be aborted and the producer keep going', async () => {
      const broker = fakeBroker();
      broker.endTxn.mockRejectedValueOnce(errorOfType('TRANSACTION_ABORTABLE')).mockResolvedValue(undefined);
      const { session, txn } = await transactionWithPartition(broker);

      await expect(txn.commit()).rejects.toMatchObject({ type: 'TRANSACTION_ABORTABLE' });
      await expect(txn.commit()).rejects.toThrow(ABORT_ONLY_MESSAGE);
      await txn.abort();

      expect(broker.endTxn).toHaveBeenLastCalledWith(expect.objectContaining({ transactionResult: false }));
      await expect(session.begin(fakeSender)).resolves.toBeTruthy();
    });

    it('lets an abort rejected with TRANSACTION_ABORTABLE be sent again', async () => {
      const broker = fakeBroker();
      broker.endTxn.mockRejectedValueOnce(errorOfType('TRANSACTION_ABORTABLE')).mockResolvedValue(undefined);
      const { session, txn } = await transactionWithPartition(broker);

      await expect(txn.abort()).rejects.toMatchObject({ type: 'TRANSACTION_ABORTABLE' });
      await txn.abort();

      expect(broker.endTxn).toHaveBeenCalledTimes(2);
      await expect(session.begin(fakeSender)).resolves.toBeTruthy();
    });
  });

  describe('epoch bump', () => {
    it('takes the producer epoch EndTxn returns under transaction version 2, without InitProducerId', async () => {
      const broker = fakeBroker();
      broker.endTxn.mockResolvedValue(mockBumpedEndTxnResponse);
      const { session, txn } = await transactionWithPartition(broker, { transactionV2: true });

      await txn.commit();

      expect(broker.endTxn).toHaveBeenCalledWith(expect.objectContaining({ isTransactionV2Enabled: true }));
      expect(broker.initProducerId).toHaveBeenCalledTimes(1);
      expect(session.ledger.getProducerEpoch()).toBe(mockBumpedEndTxnResponse.producerEpoch);
    });

    it('takes the new producer id EndTxn returns once the epoch ran out', async () => {
      const broker = fakeBroker();
      broker.endTxn.mockResolvedValue({ errorCode: 0, producerId: 2000n, producerEpoch: 0 });
      const { session, txn } = await transactionWithPartition(broker, { transactionV2: true });

      await txn.abort();

      expect(session.ledger.getProducerId()).toBe(2000n);
      expect(session.ledger.getProducerEpoch()).toBe(0);
    });

    it('settles a requested bump with the epoch EndTxn returns under transaction version 2', async () => {
      const broker = fakeBroker();
      broker.endTxn.mockResolvedValue(mockBumpedEndTxnResponse);
      const { session, txn } = await transactionWithPartition(broker, { transactionV2: true });

      session.ledger.requestEpochBump();
      session.sendFailed();
      await txn.abort();

      expect(broker.initProducerId).toHaveBeenCalledTimes(1);
      expect(session.ledger.isEpochBumpRequested()).toBe(false);
      expect(session.ledger.getProducerEpoch()).toBe(mockBumpedEndTxnResponse.producerEpoch);
    });

    it('does not bump the producer epoch outside transaction version 2', async () => {
      const broker = fakeBroker();
      const { session, txn } = await transactionWithPartition(broker);

      await txn.commit();

      expect(broker.initProducerId).toHaveBeenCalledTimes(1);
      expect(session.ledger.getProducerEpoch()).toBe(mockInitProducerIdResponse.producerEpoch);
    });

    it('bumps the epoch before the first transaction once the cluster turns transaction version 2 on', async () => {
      const broker = fakeBroker();
      broker.initProducerId
        .mockResolvedValueOnce(mockInitProducerIdResponse)
        .mockResolvedValueOnce(mockBumpedInitProducerIdResponse);
      const cluster = fakeCluster(broker);
      const session = createSession(cluster);
      await (await session.begin(fakeSender)).commit();

      cluster.brokerPool.finalizedFeatures = TRANSACTION_V2_FEATURES;
      const txn = await session.begin(fakeSender);

      expect(broker.initProducerId).toHaveBeenCalledTimes(2);
      expect(session.ledger.getProducerEpoch()).toBe(mockBumpedInitProducerIdResponse.producerEpoch);
      expect(session.isTransactionV2Enabled()).toBe(true);
      await txn.commit();
      await session.begin(fakeSender);
      expect(broker.initProducerId).toHaveBeenCalledTimes(2);
    });

    it('reports a transaction committed when the requested bump after it fails, bumping before the next one', async () => {
      const broker = fakeBroker();
      broker.initProducerId
        .mockResolvedValueOnce(mockInitProducerIdResponse)
        .mockRejectedValueOnce(new Error('coordinator down'))
        .mockResolvedValueOnce(mockBumpedInitProducerIdResponse);
      const { session, txn } = await transactionWithPartition(broker);
      session.ledger.requestEpochBump();

      await expect(txn.commit()).resolves.toBeUndefined();
      expect(broker.endTxn).toHaveBeenCalledWith(expect.objectContaining({ transactionResult: true }));
      expect(session.ledger.getProducerEpoch()).toBe(mockInitProducerIdResponse.producerEpoch);

      await session.begin(fakeSender);
      expect(session.ledger.getProducerEpoch()).toBe(mockBumpedInitProducerIdResponse.producerEpoch);
    });

    it('makes the next transaction wait for the bump still running after the previous one', async () => {
      const broker = fakeBroker();
      let finishBump!: (response: typeof mockBumpedInitProducerIdResponse) => void;
      const bumped = new Promise<typeof mockBumpedInitProducerIdResponse>((resolve) => {
        finishBump = resolve;
      });
      broker.initProducerId.mockResolvedValueOnce(mockInitProducerIdResponse).mockReturnValueOnce(bumped);
      const { session, txn } = await transactionWithPartition(broker);
      session.ledger.requestEpochBump();

      const committed = txn.commit();
      await vi.waitFor(() => expect(broker.initProducerId).toHaveBeenCalledTimes(2));
      let hasBegun = false;
      const next = session.begin(fakeSender).then((transaction) => {
        hasBegun = true;
        return transaction;
      });
      await new Promise((resolve) => setImmediate(resolve));
      expect(hasBegun).toBe(false);

      finishBump(mockBumpedInitProducerIdResponse);
      await committed;
      const nextTxn = await next;

      expect(nextTxn.isActive()).toBe(true);
      expect(broker.initProducerId).toHaveBeenCalledTimes(2);
      expect(session.ledger.getProducerEpoch()).toBe(mockBumpedInitProducerIdResponse.producerEpoch);
      expect(session.ledger.isEpochBumpRequested()).toBe(false);
    });

    it('lets only one of two transactions begun during that bump open', async () => {
      const broker = fakeBroker();
      let finishBump!: (response: typeof mockBumpedInitProducerIdResponse) => void;
      const bumped = new Promise<typeof mockBumpedInitProducerIdResponse>((resolve) => {
        finishBump = resolve;
      });
      broker.initProducerId.mockResolvedValueOnce(mockInitProducerIdResponse).mockReturnValueOnce(bumped);
      const { session, txn } = await transactionWithPartition(broker);
      session.ledger.requestEpochBump();

      const committed = txn.commit();
      await vi.waitFor(() => expect(broker.initProducerId).toHaveBeenCalledTimes(2));
      const createSecondSender = vi.fn(fakeSender);
      const first = session.begin(fakeSender);
      const second = session.begin(createSecondSender);
      finishBump(mockBumpedInitProducerIdResponse);
      await committed;

      await expect(first).resolves.toEqual(expect.objectContaining({ isActive: expect.any(Function) }));
      await expect(second).rejects.toThrow('There is already an ongoing transaction');
      expect(createSecondSender).not.toHaveBeenCalled();
    });

    it('defers a requested bump until the transaction is aborted', async () => {
      const broker = fakeBroker();
      broker.initProducerId
        .mockResolvedValueOnce(mockInitProducerIdResponse)
        .mockResolvedValueOnce(mockBumpedInitProducerIdResponse);
      const { session, txn } = await transactionWithPartition(broker);

      session.ledger.requestEpochBump();
      session.sendFailed();
      await session.ledger.bumpEpochIfRequested();
      expect(broker.initProducerId).toHaveBeenCalledTimes(1);

      await txn.abort();
      expect(broker.endTxn).toHaveBeenCalledWith(
        expect.objectContaining({ producerEpoch: mockInitProducerIdResponse.producerEpoch, transactionResult: false }),
      );
      expect(broker.initProducerId).toHaveBeenCalledTimes(2);
      expect(session.ledger.getProducerEpoch()).toBe(mockBumpedInitProducerIdResponse.producerEpoch);
    });
  });

  describe('sendOffsets', () => {
    const consumerGroupId = 'consumer-group-id';
    const topics = [{ topic, partitions: [{ partition: 0, offset: 5n }] }];

    it('adds the offsets to the transaction and marks them committed locally once it commits', async () => {
      const broker = fakeBroker();
      const cluster = fakeCluster(broker);
      const session = createSession(cluster);
      const txn = await session.begin(fakeSender);

      await txn.sendOffsets({ consumerGroupId, topics });

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
        topics: [{ topic, partitions: [{ partition: 0, offset: 5n, metadata: null }] }],
        isTransactionV2Enabled: false,
      });
      expect(cluster.markOffsetAsCommitted).not.toHaveBeenCalled();

      await txn.commit();
      expect(broker.endTxn).toHaveBeenCalledWith(expect.objectContaining({ transactionResult: true }));
      expect(cluster.markOffsetAsCommitted).toHaveBeenCalledExactlyOnceWith({
        groupId: consumerGroupId,
        topic,
        partition: 0,
        offset: 5n,
      });
    });

    it('adds the offsets with TxnOffsetCommit alone under transaction version 2', async () => {
      const broker = fakeBroker();
      const cluster = fakeCluster(broker, { transactionV2: true });
      const txn = await createSession(cluster).begin(fakeSender);

      await txn.sendOffsets({ consumerGroupId, topics });

      expect(broker.addOffsetsToTxn).not.toHaveBeenCalled();
      expect(broker.txnOffsetCommit).toHaveBeenCalledWith(expect.objectContaining({ isTransactionV2Enabled: true }));
      await txn.commit();
      expect(broker.endTxn).toHaveBeenCalledWith(expect.objectContaining({ transactionResult: true }));
      expect(cluster.markOffsetAsCommitted).toHaveBeenCalledOnce();
    });

    describe('topic ids', () => {
      const ordersId = Buffer.alloc(16, 1);
      /** A Kafka 4.4+ cluster with transaction version 2, which can take TxnOffsetCommit v6. */
      const topicIdCluster = { transactionV2: true, txnOffsetCommitMaxVersion: 6 };

      it('loads the topics into the metadata and sends their ids on Kafka 4.4+ under transaction version 2', async () => {
        const broker = fakeBroker();
        const cluster = fakeCluster(broker, topicIdCluster);
        cluster.findTopicId.mockReturnValue(ordersId);
        const txn = await createSession(cluster).begin(fakeSender);

        await txn.sendOffsets({ consumerGroupId, topics });

        expect(cluster.addMultipleTargetTopics).toHaveBeenCalledExactlyOnceWith([topic]);
        expect(broker.txnOffsetCommit).toHaveBeenCalledWith(
          expect.objectContaining({
            topics: [{ topic, topicId: ordersId, partitions: [{ partition: 0, offset: 5n, metadata: null }] }],
          }),
        );
      });

      it.each([
        ['without transaction version 2', { transactionV2: false, txnOffsetCommitMaxVersion: 6 }],
        ['on a cluster without TxnOffsetCommit v6', { transactionV2: true, txnOffsetCommitMaxVersion: 5 }],
      ])('does not load the topics into the metadata %s', async (_case, options) => {
        const broker = fakeBroker();
        const cluster = fakeCluster(broker, options);
        const txn = await createSession(cluster).begin(fakeSender);

        await txn.sendOffsets({ consumerGroupId, topics });

        expect(cluster.addMultipleTargetTopics).not.toHaveBeenCalled();
        expect(broker.txnOffsetCommit).toHaveBeenCalledOnce();
      });

      it('sends the offsets by topic name when the topics cannot be loaded', async () => {
        const broker = fakeBroker();
        const cluster = fakeCluster(broker, topicIdCluster);
        cluster.addMultipleTargetTopics.mockRejectedValue(errorOfType('TOPIC_AUTHORIZATION_FAILED'));
        cluster.brokerPool.metadata = null;
        const txn = await createSession(cluster).begin(fakeSender);

        await txn.sendOffsets({ consumerGroupId, topics });

        expect(cluster.findTopicId).not.toHaveBeenCalled();
        expect(broker.txnOffsetCommit).toHaveBeenCalledWith(
          expect.objectContaining({ topics: [expect.objectContaining({ topicId: undefined })] }),
        );
      });

      it('retries UNKNOWN_TOPIC_ID with the id the refreshed metadata holds', async () => {
        const recreatedId = Buffer.alloc(16, 2);
        const broker = fakeBroker();
        broker.txnOffsetCommit.mockRejectedValueOnce(errorOfType('UNKNOWN_TOPIC_ID')).mockResolvedValueOnce(undefined);
        const cluster = fakeCluster(broker, topicIdCluster);
        cluster.findTopicId.mockReturnValueOnce(ordersId).mockReturnValue(recreatedId);
        const txn = await createSession(cluster).begin(fakeSender);

        await txn.sendOffsets({ consumerGroupId, topics });

        expect(cluster.recover).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({ type: 'UNKNOWN_TOPIC_ID' }));
        expect(broker.txnOffsetCommit).toHaveBeenCalledTimes(2);
        expect(broker.txnOffsetCommit).toHaveBeenLastCalledWith(
          expect.objectContaining({ topics: [expect.objectContaining({ topicId: recreatedId })] }),
        );
      });
    });

    it('aborts through EndTxn when TxnOffsetCommit fails under transaction version 2', async () => {
      const broker = fakeBroker();
      broker.txnOffsetCommit.mockRejectedValue(errorOfType('TRANSACTION_ABORTABLE'));
      const session = createSession(fakeCluster(broker, { transactionV2: true }));
      const txn = await session.begin(fakeSender);

      await expect(txn.sendOffsets({ consumerGroupId, topics })).rejects.toMatchObject({
        type: 'TRANSACTION_ABORTABLE',
      });
      await expect(txn.commit()).rejects.toEqual(new KafkaNonRetriableError(ABORT_ONLY_MESSAGE));

      // The broker may have added the group before it failed, so the abort still reaches it.
      await txn.abort();
      expect(broker.endTxn).toHaveBeenCalledWith(expect.objectContaining({ transactionResult: false }));
      await expect(session.begin(fakeSender)).resolves.toBeTruthy();
    });

    it('does not mark offsets committed when the transaction aborts', async () => {
      const broker = fakeBroker();
      const cluster = fakeCluster(broker);
      const session = createSession(cluster);
      const aborted = await session.begin(fakeSender);
      await aborted.sendOffsets({ consumerGroupId, topics });

      await aborted.abort();
      const next = await session.begin(fakeSender);
      await next.commit();

      expect(broker.endTxn).toHaveBeenCalledWith(expect.objectContaining({ transactionResult: false }));
      expect(cluster.markOffsetAsCommitted).not.toHaveBeenCalled();
    });

    it('does not mark offsets of a call that finishes after its transaction ended', async () => {
      const broker = fakeBroker();
      let finishTxnOffsetCommit: (() => void) | undefined;
      broker.txnOffsetCommit.mockImplementationOnce(
        () =>
          new Promise<void>((resolve) => {
            finishTxnOffsetCommit = resolve;
          }),
      );
      const cluster = fakeCluster(broker);
      const session = createSession(cluster);
      const ended = await session.begin(fakeSender);
      const sending = ended.sendOffsets({ consumerGroupId, topics });
      await vi.waitFor(() => expect(finishTxnOffsetCommit).toBeDefined());

      await ended.abort();
      finishTxnOffsetCommit?.();
      await sending;
      const next = await session.begin(fakeSender);
      await session.enrollPartitions([{ topic, partitions: [{ partition: 0 }] }]);
      await next.commit();

      expect(cluster.markOffsetAsCommitted).not.toHaveBeenCalled();
    });

    it('marks offsets committed only once a failed commit is retried successfully', async () => {
      const broker = fakeBroker();
      broker.endTxn.mockRejectedValueOnce(new Error('connection lost'));
      const cluster = fakeCluster(broker);
      const txn = await createSession(cluster).begin(fakeSender);
      await txn.sendOffsets({ consumerGroupId, topics });

      await expect(txn.commit()).rejects.toThrow('connection lost');
      expect(cluster.markOffsetAsCommitted).not.toHaveBeenCalled();

      await txn.commit();
      expect(cluster.markOffsetAsCommitted).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ groupId: consumerGroupId, partition: 0, offset: 5n }),
      );
    });

    it('does not mark offsets committed when TxnOffsetCommit fails', async () => {
      const broker = fakeBroker();
      broker.txnOffsetCommit.mockRejectedValue(errorOfType('INVALID_PRODUCER_EPOCH'));
      const cluster = fakeCluster(broker);
      const txn = await createSession(cluster).begin(fakeSender);

      await expect(txn.sendOffsets({ consumerGroupId, topics })).rejects.toMatchObject({
        type: 'INVALID_PRODUCER_EPOCH',
      });
      expect(cluster.markOffsetAsCommitted).not.toHaveBeenCalled();
    });

    it.each(['CONCURRENT_TRANSACTIONS', 'GROUP_LOAD_IN_PROGRESS'])(
      'retries AddOffsetsToTxn on %s, as right after the previous transaction ended',
      async (type) => {
        const broker = fakeBroker();
        broker.addOffsetsToTxn.mockRejectedValueOnce(errorOfType(type)).mockResolvedValueOnce(undefined);
        const txn = await createSession(fakeCluster(broker), { initialRetryTime: 1 }).begin(fakeSender);

        await txn.sendOffsets({ consumerGroupId, topics });
        expect(broker.addOffsetsToTxn).toHaveBeenCalledTimes(2);
        expect(broker.txnOffsetCommit).toHaveBeenCalledOnce();
      },
    );

    it('sends AddOffsetsToTxn to a fresh transaction coordinator when the current one moved', async () => {
      const staleBroker = fakeBroker();
      staleBroker.addOffsetsToTxn.mockRejectedValue(errorOfType('NOT_COORDINATOR_FOR_GROUP'));
      const freshBroker = fakeBroker();
      const cluster = fakeCluster(staleBroker);
      cluster.findGroupCoordinator = vi
        .fn()
        .mockResolvedValueOnce(staleBroker) // transaction coordinator lookup, during begin
        .mockResolvedValueOnce(staleBroker) // transaction coordinator lookup, first AddOffsetsToTxn
        .mockResolvedValue(freshBroker); // every lookup after the refresh
      const txn = await createSession(cluster, { initialRetryTime: 1 }).begin(fakeSender);

      await txn.sendOffsets({ consumerGroupId, topics });
      expect(staleBroker.addOffsetsToTxn).toHaveBeenCalledOnce();
      expect(freshBroker.addOffsetsToTxn).toHaveBeenCalledOnce();
      expect(cluster.recover).toHaveBeenCalledOnce();
    });

    it.each([
      ['AddOffsetsToTxn', 'GROUP_AUTHORIZATION_FAILED', 'addOffsetsToTxn'],
      ['TxnOffsetCommit', 'GROUP_AUTHORIZATION_FAILED', 'txnOffsetCommit'],
      ['AddOffsetsToTxn', 'TRANSACTION_ABORTABLE', 'addOffsetsToTxn'],
      ['TxnOffsetCommit', 'TRANSACTION_ABORTABLE', 'txnOffsetCommit'],
      // TxnOffsetCommit v6+ (KIP-1319); older versions answer ILLEGAL_GENERATION for both.
      ['TxnOffsetCommit', 'GROUP_ID_NOT_FOUND', 'txnOffsetCommit'],
      ['TxnOffsetCommit', 'STALE_MEMBER_EPOCH', 'txnOffsetCommit'],
    ] as const)('leaves the transaction abort-only when %s fails with %s', async (_api, type, request) => {
      const broker = fakeBroker();
      broker[request].mockRejectedValue(errorOfType(type));
      const { session, txn } = await transactionWithPartition(broker);

      await expect(txn.sendOffsets({ consumerGroupId, topics })).rejects.toMatchObject({ type });
      expect(broker[request]).toHaveBeenCalledOnce();
      expect(txn.isActive()).toBe(false);
      await expect(txn.commit()).rejects.toEqual(new KafkaNonRetriableError(ABORT_ONLY_MESSAGE));
      expect(broker.endTxn).not.toHaveBeenCalled();

      await txn.abort();
      expect(broker.endTxn).toHaveBeenCalledWith(expect.objectContaining({ transactionResult: false }));
      await expect(session.begin(fakeSender)).resolves.toBeTruthy();
    });

    it('rejects the next begin until the transaction whose sendOffsets failed is aborted', async () => {
      const broker = fakeBroker();
      broker.txnOffsetCommit.mockRejectedValueOnce(errorOfType('GROUP_AUTHORIZATION_FAILED'));
      const session = createSession(fakeCluster(broker));
      const txn = await session.begin(fakeSender);
      await expect(txn.sendOffsets({ consumerGroupId, topics })).rejects.toThrow();

      await expect(session.begin(fakeSender)).rejects.toEqual(new KafkaNonRetriableError(ABORT_BEFORE_BEGIN_MESSAGE));

      await txn.abort();
      await expect(session.begin(fakeSender)).resolves.toMatchObject({});
    });

    it('does not make the next transaction abort-only when a call of an ended one fails late', async () => {
      const broker = fakeBroker();
      let failTxnOffsetCommit: ((error: Error) => void) | undefined;
      broker.txnOffsetCommit.mockImplementationOnce(
        () =>
          new Promise<void>((_resolve, reject) => {
            failTxnOffsetCommit = reject;
          }),
      );
      const session = createSession(fakeCluster(broker));
      const ended = await session.begin(fakeSender);
      const sending = ended.sendOffsets({ consumerGroupId, topics });
      await vi.waitFor(() => expect(failTxnOffsetCommit).toBeDefined());

      await ended.abort();
      const next = await session.begin(fakeSender);
      failTxnOffsetCommit?.(errorOfType('GROUP_AUTHORIZATION_FAILED'));
      await expect(sending).rejects.toThrow();

      expect(next.isActive()).toBe(true);
    });

    it('retries TxnOffsetCommit under transaction version 2 while the previous transaction is still ending', async () => {
      const broker = fakeBroker();
      broker.txnOffsetCommit
        .mockRejectedValueOnce(errorOfType('CONCURRENT_TRANSACTIONS'))
        .mockResolvedValueOnce(undefined);
      const txn = await createSession(fakeCluster(broker, { transactionV2: true }), { initialRetryTime: 1 }).begin(
        fakeSender,
      );

      await txn.sendOffsets({ consumerGroupId, topics });
      expect(broker.txnOffsetCommit).toHaveBeenCalledTimes(2);
    });

    it('retries while the group coordinator is still loading', async () => {
      const broker = fakeBroker();
      broker.txnOffsetCommit
        .mockRejectedValueOnce(Object.assign(new Error('loading'), { type: 'GROUP_LOAD_IN_PROGRESS' }))
        .mockResolvedValueOnce(undefined);
      const txn = await createSession(fakeCluster(broker)).begin(fakeSender);

      await txn.sendOffsets({ consumerGroupId, topics });
      expect(broker.txnOffsetCommit).toHaveBeenCalledTimes(2);
    });

    it('looks up a fresh group coordinator and retries when the current one is stale', async () => {
      const staleBroker = fakeBroker();
      staleBroker.txnOffsetCommit.mockRejectedValue(
        Object.assign(new Error('stale'), { type: 'NOT_COORDINATOR_FOR_GROUP' }),
      );
      const freshBroker = fakeBroker();
      const cluster = fakeCluster(staleBroker);
      cluster.findGroupCoordinator = vi
        .fn()
        .mockResolvedValueOnce(staleBroker) // transaction coordinator lookup, during begin
        .mockResolvedValueOnce(staleBroker) // transaction coordinator lookup, during sendOffsets
        .mockResolvedValueOnce(staleBroker) // group coordinator lookup, first attempt
        .mockResolvedValueOnce(freshBroker); // group coordinator lookup, after refresh
      const txn = await createSession(cluster).begin(fakeSender);

      await txn.sendOffsets({ consumerGroupId, topics });
      expect(staleBroker.txnOffsetCommit).toHaveBeenCalledTimes(1);
      expect(freshBroker.txnOffsetCommit).toHaveBeenCalledTimes(1);
    });
  });

  describe('flush', () => {
    it("flushes the open transaction's sender", async () => {
      const session = createSession(fakeCluster(fakeBroker()));
      await expect(session.flush()).resolves.toBeUndefined();

      const sender = fakeSender();
      await session.begin(() => sender);
      await session.flush();
      expect(sender.flush).toHaveBeenCalledTimes(1);
    });
  });
});
