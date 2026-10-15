import { describe, expect, it, vi } from 'vitest';
import type { Cluster, PartitionMetadata } from '../cluster/index';
import { KafkaRequestTimeoutError } from '../errors';
import { createErrorFromCode, ERROR_CODES } from '../protocol/error-codes';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { retrier } from '../retry/index';
import { createEosManager, type EosManager } from './eos-manager/index';
import { createNodeLatencyTracker } from './node-latency-tracker';
import { createSendMessages } from './send-messages';
import type { PartitionerArgs } from './types';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });
const topic = 'topic-name';

function fakeProduceResponse(topicName: string, partition: number) {
  return {
    throttleTime: 0,
    clientSideThrottleTime: 0,
    topics: [
      {
        topicName,
        partitions: [
          { partition, errorCode: 0, baseOffset: BigInt(partition), logAppendTime: -1n, logStartOffset: 0n },
        ],
      },
    ],
  };
}

function fakeBroker(nodeId: number) {
  return { nodeId, produce: vi.fn().mockImplementation(() => Promise.resolve(fakeProduceResponse(topic, nodeId - 1))) };
}

function fakePartitionMetadata(partitionId: number): PartitionMetadata {
  return {
    partitionErrorCode: 0,
    partitionId,
    leader: partitionId,
    replicas: [partitionId],
    isr: [partitionId],
    offlineReplicas: [],
  };
}

function fakeEosManager(overrides: Partial<Record<keyof EosManager, unknown>> = {}) {
  return {
    getProducerId: vi.fn().mockReturnValue(-1n),
    getProducerEpoch: vi.fn().mockReturnValue(-1),
    getSequence: vi.fn().mockReturnValue(0),
    getTransactionalId: vi.fn().mockReturnValue(undefined),
    updateSequence: vi.fn(),
    isTransactional: vi.fn().mockReturnValue(false),
    isInitialized: vi.fn().mockReturnValue(false),
    initProducerId: vi.fn().mockResolvedValue(undefined),
    addPartitionsToTransaction: vi.fn().mockResolvedValue(undefined),
    markTransactionAbortable: vi.fn(),
    requestEpochBump: vi.fn(),
    bumpEpochIfRequested: vi.fn().mockResolvedValue(undefined),
    acquirePartitionGates: vi.fn().mockResolvedValue(undefined),
    releasePartitionGates: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  } as unknown as EosManager;
}

/** Assigns message `i` to partition `i % 3`, so 9 messages spread evenly across partitions 0/1/2. */
const cyclingPartitioner = ({ message }: PartitionerArgs): number => Number(message.key ?? 0) % 3;

function ninePartitionedMessages() {
  return Array.from({ length: 9 }, (_, i) => ({ value: `v${i}`, key: String(i) }));
}

describe('producer/sendMessages', () => {
  const partitionsPerLeader = { 1: [0], 2: [1], 3: [2] };
  const partitionMetadata = [fakePartitionMetadata(0), fakePartitionMetadata(1), fakePartitionMetadata(2)];

  function fakeCluster(brokers: Record<number, ReturnType<typeof fakeBroker>>) {
    return {
      addMultipleTargetTopics: vi.fn().mockResolvedValue(undefined),
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
      refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
      applyLeaderUpdate: vi.fn().mockResolvedValue(true),
      findTopicPartitionMetadata: vi.fn().mockReturnValue(partitionMetadata),
      findTopicId: vi.fn().mockReturnValue(undefined),
      findLeaderForPartitions: vi.fn().mockImplementation((_topic: string, partitions: readonly number[]) => {
        const requested = new Set(partitions);
        const result: Record<number, number[]> = {};
        for (const [nodeId, nodePartitions] of Object.entries(partitionsPerLeader)) {
          const matching = nodePartitions.filter((partition) => requested.has(partition));
          if (matching.length > 0) result[Number(nodeId)] = matching;
        }
        return result;
      }),
      findBroker: vi
        .fn()
        .mockImplementation(({ nodeId }: { nodeId: string }) => Promise.resolve(brokers[Number(nodeId)])),
      removeBroker: vi.fn(),
      connect: vi.fn().mockResolvedValue(undefined),
      targetTopics: new Set<string>(),
      isConnected: vi.fn().mockReturnValue(true),
      recordProduceMetrics: vi.fn(),
    };
  }

  it('retries only failed brokers and keeps successful partition acks', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    brokers[1].produce
      .mockImplementationOnce(() => Promise.reject(createErrorFromCode(5)))
      .mockImplementation(() => Promise.resolve(fakeProduceResponse(topic, 0)));
    brokers[3].produce
      .mockImplementationOnce(() => Promise.reject(createErrorFromCode(5)))
      .mockImplementationOnce(() => Promise.reject(createErrorFromCode(5)))
      .mockImplementation(() => Promise.resolve(fakeProduceResponse(topic, 2)));

    const cluster = fakeCluster(brokers);
    const eosManager = fakeEosManager();
    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager,
      retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
    });

    const response = await sendMessages({
      acks: -1,
      timeout: 30_000,
      topicMessages: [{ topic, messages: ninePartitionedMessages() }],
    });

    expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalled();
    expect(eosManager.addPartitionsToTransaction).not.toHaveBeenCalled();

    expect(brokers[1].produce).toHaveBeenCalledTimes(2);
    expect(brokers[2].produce).toHaveBeenCalledTimes(1);
    expect(brokers[3].produce).toHaveBeenCalledTimes(3);
    expect(response).toEqual([
      { topicName: topic, partition: 0, errorCode: 0, baseOffset: 0n, logAppendTime: -1n, logStartOffset: 0n },
      { topicName: topic, partition: 1, errorCode: 0, baseOffset: 1n, logAppendTime: -1n, logStartOffset: 0n },
      { topicName: topic, partition: 2, errorCode: 0, baseOffset: 2n, logAppendTime: -1n, logStartOffset: 0n },
    ]);
  });

  const PRODUCE_ERRORS = ['UNKNOWN_TOPIC_OR_PARTITION', 'LEADER_NOT_AVAILABLE', 'NOT_LEADER_OR_FOLLOWER'];

  for (const errorType of PRODUCE_ERRORS) {
    it(`refreshes stale metadata on ${errorType}`, async () => {
      const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
      const code = ERROR_CODES.find((entry) => entry.type === errorType)!.code;
      brokers[1].produce
        .mockImplementationOnce(() => Promise.reject(createErrorFromCode(code)))
        .mockImplementationOnce(() => Promise.resolve(fakeProduceResponse(topic, 0)));

      const cluster = fakeCluster(brokers);
      const sendMessages = createSendMessages({
        logger: silentLogger,
        cluster: cluster as unknown as Cluster,
        partitioner: cyclingPartitioner,
        eosManager: fakeEosManager(),
        retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
      });

      await sendMessages({
        acks: -1,
        timeout: 30_000,
        topicMessages: [{ topic, messages: ninePartitionedMessages() }],
      });

      expect(brokers[1].produce).toHaveBeenCalledTimes(2);
      expect(cluster.refreshMetadata).toHaveBeenCalled();
    });
  }

  it('recovers the leader from a CurrentLeader hint instead of a full metadata refresh (KIP-951)', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    const code = ERROR_CODES.find((entry) => entry.type === 'NOT_LEADER_OR_FOLLOWER')!.code;
    const error = createErrorFromCode(code, {
      topic,
      partition: 0,
      currentLeader: { leaderId: 2, leaderEpoch: 5 },
      nodeEndpoints: [{ nodeId: 2, host: 'broker-2', port: 9093, rack: null }],
    });
    brokers[1].produce
      .mockImplementationOnce(() => Promise.reject(error))
      .mockImplementationOnce(() => Promise.resolve(fakeProduceResponse(topic, 0)));

    const cluster = fakeCluster(brokers);
    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager: fakeEosManager(),
      retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
    });

    await sendMessages({
      acks: -1,
      timeout: 30_000,
      topicMessages: [{ topic, messages: ninePartitionedMessages() }],
    });

    expect(cluster.applyLeaderUpdate).toHaveBeenCalledWith({
      topic,
      partition: 0,
      currentLeader: { leaderId: 2, leaderEpoch: 5 },
      nodeEndpoints: [{ nodeId: 2, host: 'broker-2', port: 9093, rack: null }],
    });
    expect(cluster.refreshMetadata).not.toHaveBeenCalled();
    expect(brokers[1].produce).toHaveBeenCalledTimes(2);
  });

  it('refreshes metadata if partition metadata is empty', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    const cluster = fakeCluster(brokers);
    cluster.findTopicPartitionMetadata.mockReturnValueOnce([]).mockReturnValue(partitionMetadata);

    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager: fakeEosManager(),
      retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
    });

    await sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: ninePartitionedMessages() }] });

    expect(cluster.refreshMetadata).toHaveBeenCalled();
  });

  it('retrieves sequence information from the transaction manager and updates it', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    const cluster = fakeCluster(brokers);
    const eosManager = fakeEosManager({ getSequence: vi.fn().mockReturnValue(5) });

    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager,
      retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
    });

    await sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: ninePartitionedMessages() }] });

    for (const nodeId of [1, 2, 3]) {
      const request = brokers[nodeId as 1 | 2 | 3].produce.mock.calls[0]?.[0];
      expect(request.topicData[0]?.partitions[0]).toEqual(expect.objectContaining({ firstSequence: 5 }));
    }

    expect(eosManager.updateSequence).toHaveBeenCalledWith(topic, 0, 3);
    expect(eosManager.updateSequence).toHaveBeenCalledWith(topic, 1, 3);
    expect(eosManager.updateSequence).toHaveBeenCalledWith(topic, 2, 3);
    expect(eosManager.acquirePartitionGates).toHaveBeenCalled();
    expect(eosManager.releasePartitionGates).toHaveBeenCalled();
  });

  it('rolls back sequence tracking when the broker rejects a produce under a stable epoch', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    const code = ERROR_CODES.find((entry) => entry.type === 'MESSAGE_TOO_LARGE')!.code;
    brokers[1].produce.mockImplementationOnce(() => Promise.reject(createErrorFromCode(code, { topic, partition: 0 })));

    const cluster = fakeCluster(brokers);
    const eosManager = fakeEosManager({ getProducerEpoch: vi.fn().mockReturnValue(0) });

    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager,
      retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
    });

    await expect(
      sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: [{ value: 'v0', key: '0' }] }] }),
    ).rejects.toMatchObject({ type: 'MESSAGE_TOO_LARGE' });

    expect(eosManager.updateSequence).toHaveBeenCalledWith(topic, 0, 1);
    expect(eosManager.updateSequence).toHaveBeenCalledWith(topic, 0, -1);
    expect(eosManager.requestEpochBump).not.toHaveBeenCalled();
  });

  it('does not roll back sequence tracking across a concurrent producer epoch reset', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    let epoch = 0;
    brokers[1].produce.mockImplementationOnce(() => {
      // Simulate a concurrent InitProducerId (e.g. another partition's UNKNOWN_PRODUCER_ID
      // recovery) completing while this request is in flight: it bumps the epoch and resets
      // sequence tracking to zero before this request's own failure is handled.
      epoch = 1;
      return Promise.reject(new Error('stale epoch'));
    });

    const cluster = fakeCluster(brokers);
    const eosManager = fakeEosManager({ getProducerEpoch: vi.fn(() => epoch) });

    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager,
      retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
    });

    await expect(
      sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: [{ value: 'v0', key: '0' }] }] }),
    ).rejects.toThrow('stale epoch');

    expect(brokers[1].produce).toHaveBeenCalledWith(expect.objectContaining({ producerEpoch: 0 }));
    // The optimistic increment happened under epoch 0, but by the time the failure is handled
    // the epoch (and the sequence map behind it) already belongs to generation 1 — rolling back
    // here would drive the freshly-reset sequence negative instead of leaving it alone.
    expect(eosManager.updateSequence).toHaveBeenCalledWith(topic, 0, 1);
    expect(eosManager.updateSequence).not.toHaveBeenCalledWith(topic, 0, -1);
  });

  it('adds partitions to the transaction when transactional', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    const cluster = fakeCluster(brokers);
    const eosManager = fakeEosManager({ isTransactional: vi.fn().mockReturnValue(true) });

    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager,
      retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
    });

    await sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: ninePartitionedMessages() }] });

    expect(eosManager.addPartitionsToTransaction).toHaveBeenCalledTimes(3);
    expect(eosManager.addPartitionsToTransaction).toHaveBeenCalledWith([
      { topic, partitions: [expect.objectContaining({ partition: 0 })] },
    ]);
    expect(eosManager.addPartitionsToTransaction).toHaveBeenCalledWith([
      { topic, partitions: [expect.objectContaining({ partition: 1 })] },
    ]);
    expect(eosManager.addPartitionsToTransaction).toHaveBeenCalledWith([
      { topic, partitions: [expect.objectContaining({ partition: 2 })] },
    ]);
  });

  it('produces with the transactional id and producer id & epoch when transactional', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    const cluster = fakeCluster(brokers);
    const eosManager = fakeEosManager({
      isTransactional: vi.fn().mockReturnValue(true),
      getProducerId: vi.fn().mockReturnValue(1000n),
      getProducerEpoch: vi.fn().mockReturnValue(1),
      getTransactionalId: vi.fn().mockReturnValue('transactionalid'),
    });

    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager,
      retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
    });

    await sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: ninePartitionedMessages() }] });

    for (const nodeId of [1, 2, 3]) {
      expect(brokers[nodeId as 1 | 2 | 3].produce).toHaveBeenCalledWith(
        expect.objectContaining({ producerId: 1000n, producerEpoch: 1, transactionalId: 'transactionalid' }),
      );
    }
  });

  it('produces with the producer id & epoch but no transactional id when idempotent', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    const cluster = fakeCluster(brokers);
    const eosManager = fakeEosManager({
      isTransactional: vi.fn().mockReturnValue(false),
      getProducerId: vi.fn().mockReturnValue(1000n),
      getProducerEpoch: vi.fn().mockReturnValue(1),
      getTransactionalId: vi.fn().mockReturnValue('transactionalid'),
    });

    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager,
      retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
    });

    await sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: ninePartitionedMessages() }] });

    for (const nodeId of [1, 2, 3]) {
      expect(brokers[nodeId as 1 | 2 | 3].produce).toHaveBeenCalledWith(
        expect.objectContaining({ producerId: 1000n, producerEpoch: 1, transactionalId: undefined }),
      );
    }
  });

  it('returns an empty response for acks: 0', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    const cluster = fakeCluster(brokers);

    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager: fakeEosManager(),
      retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
    });

    const response = await sendMessages({
      acks: 0,
      timeout: 30_000,
      topicMessages: [{ topic, messages: ninePartitionedMessages() }],
    });
    expect(response).toEqual([]);
  });

  it('keeps a sibling broker error-0 ack after NOT_LEADER_OR_FOLLOWER', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    const notLeader = ERROR_CODES.find((entry) => entry.type === 'NOT_LEADER_OR_FOLLOWER')!.code;
    brokers[2].produce
      .mockImplementationOnce(() => Promise.reject(createErrorFromCode(notLeader)))
      .mockImplementation(() => Promise.resolve(fakeProduceResponse(topic, 1)));

    const cluster = fakeCluster(brokers);
    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager: fakeEosManager(),
      retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
    });

    const response = await sendMessages({
      acks: -1,
      timeout: 30_000,
      topicMessages: [{ topic, messages: ninePartitionedMessages() }],
    });

    expect(cluster.refreshMetadata).toHaveBeenCalled();
    expect(brokers[1].produce).toHaveBeenCalledTimes(1);
    expect(brokers[3].produce).toHaveBeenCalledTimes(1);
    expect(brokers[2].produce).toHaveBeenCalledTimes(2);
    expect(response).toEqual([
      { topicName: topic, partition: 0, errorCode: 0, baseOffset: 0n, logAppendTime: -1n, logStartOffset: 0n },
      { topicName: topic, partition: 1, errorCode: 0, baseOffset: 1n, logAppendTime: -1n, logStartOffset: 0n },
      { topicName: topic, partition: 2, errorCode: 0, baseOffset: 2n, logAppendTime: -1n, logStartOffset: 0n },
    ]);
  });

  it('refreshes metadata when a produce request times out', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    brokers[1].produce
      .mockImplementationOnce(() =>
        Promise.reject(new KafkaRequestTimeoutError('Request timed out', { broker: 'h:1' })),
      )
      .mockImplementation(() => Promise.resolve(fakeProduceResponse(topic, 0)));

    const cluster = fakeCluster(brokers);
    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager: fakeEosManager(),
      retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
    });

    await sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: ninePartitionedMessages() }] });

    expect(cluster.refreshMetadata).toHaveBeenCalled();
  });

  it('reallocates the producer id and retries produce on UNKNOWN_PRODUCER_ID', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    const code = ERROR_CODES.find((entry) => entry.type === 'UNKNOWN_PRODUCER_ID')!.code;
    brokers[1].produce
      .mockImplementationOnce(() => Promise.reject(createErrorFromCode(code)))
      .mockImplementationOnce(() => Promise.resolve(fakeProduceResponse(topic, 0)));

    const cluster = fakeCluster(brokers);
    const eosManager = fakeEosManager({
      isInitialized: vi.fn().mockReturnValue(true),
      initProducerId: vi.fn().mockResolvedValue(undefined),
    });
    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager,
      retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
    });

    await sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: ninePartitionedMessages() }] });

    expect(eosManager.initProducerId).toHaveBeenCalledTimes(1);
    expect(brokers[1].produce).toHaveBeenCalledTimes(2);
  });

  it('marks the transaction abort-only on TRANSACTION_ABORTABLE (KIP-890)', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    const code = ERROR_CODES.find((entry) => entry.type === 'TRANSACTION_ABORTABLE')!.code;
    brokers[1].produce.mockRejectedValue(createErrorFromCode(code));

    const cluster = fakeCluster(brokers);
    const eosManager = fakeEosManager({ isTransactional: vi.fn().mockReturnValue(true) });
    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager,
      retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
    });

    await expect(
      sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: ninePartitionedMessages() }] }),
    ).rejects.toMatchObject({ type: 'TRANSACTION_ABORTABLE' });

    expect(eosManager.markTransactionAbortable).toHaveBeenCalled();
  });

  it('marks the transaction abort-only when a transactional send fails after a partial write', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    brokers[1].produce.mockRejectedValue(createErrorFromCode(5));

    const eosManager = fakeEosManager({ isTransactional: vi.fn().mockReturnValue(true) });
    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: fakeCluster(brokers) as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager,
      retrier: retrier({ retries: 1, initialRetryTime: 1, maxRetryTime: 5 }),
    });

    await expect(
      sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: ninePartitionedMessages() }] }),
    ).rejects.toMatchObject({ name: 'KafkaNumberOfRetriesExceeded' });

    // Partitions 1 and 2 were written inside the transaction; committing now would keep a partial send.
    expect(brokers[2].produce).toHaveBeenCalledTimes(1);
    expect(eosManager.markTransactionAbortable).toHaveBeenCalled();
  });

  it('does not recover UNKNOWN_PRODUCER_ID when the producer is not initialized', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    const code = ERROR_CODES.find((entry) => entry.type === 'UNKNOWN_PRODUCER_ID')!.code;
    brokers[1].produce.mockImplementationOnce(() => Promise.reject(createErrorFromCode(code)));

    const cluster = fakeCluster(brokers);
    const eosManager = fakeEosManager();
    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager,
      retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
    });

    await expect(
      sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: ninePartitionedMessages() }] }),
    ).rejects.toMatchObject({ type: 'UNKNOWN_PRODUCER_ID' });

    expect(eosManager.initProducerId).not.toHaveBeenCalled();
  });

  it('retries only the failed topic-partition and keeps merge order stable', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    brokers[1].produce
      .mockImplementationOnce(() => Promise.reject(createErrorFromCode(5)))
      .mockImplementation(() => Promise.resolve(fakeProduceResponse(topic, 0)));

    const cluster = fakeCluster(brokers);
    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager: fakeEosManager(),
      retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
    });

    const response = await sendMessages({
      acks: -1,
      timeout: 30_000,
      topicMessages: [{ topic, messages: ninePartitionedMessages() }],
    });

    expect(brokers[1].produce).toHaveBeenCalledTimes(2);
    expect(brokers[2].produce).toHaveBeenCalledTimes(1);
    expect(brokers[3].produce).toHaveBeenCalledTimes(1);

    const retryTopicData = brokers[1].produce.mock.calls[1]?.[0]?.topicData as Array<{
      partitions: Array<{ partition: number }>;
    }>;
    expect(retryTopicData[0]?.partitions.map((entry) => entry.partition)).toEqual([0]);
    expect(response.map((entry) => entry.partition)).toEqual([0, 1, 2]);
  });

  it('does not drop the error when retries are exhausted after a partial success', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    brokers[1].produce.mockRejectedValue(createErrorFromCode(5));

    const cluster = fakeCluster(brokers);
    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager: fakeEosManager(),
      retrier: retrier({ retries: 1, initialRetryTime: 1, maxRetryTime: 5 }),
    });

    await expect(
      sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: ninePartitionedMessages() }] }),
    ).rejects.toMatchObject({ name: 'KafkaNumberOfRetriesExceeded' });

    expect(brokers[2].produce).toHaveBeenCalledTimes(1);
    expect(brokers[3].produce).toHaveBeenCalledTimes(1);
  });

  it('plumbs topicId from cluster metadata onto Produce topicData', async () => {
    const topicId = Buffer.from('0123456789abcdef');
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    const cluster = fakeCluster(brokers);
    cluster.findTopicId.mockReturnValue(topicId);

    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager: fakeEosManager(),
      retrier: retrier({ retries: 1, initialRetryTime: 1, maxRetryTime: 5 }),
    });

    await sendMessages({
      acks: -1,
      timeout: 30_000,
      topicMessages: [{ topic, messages: ninePartitionedMessages() }],
    });

    expect(brokers[1].produce).toHaveBeenCalledWith(
      expect.objectContaining({
        topicData: [expect.objectContaining({ topic, topicId })],
      }),
    );
  });

  describe('nodeLatencyTracker', () => {
    it('records latency for the responding node after a successful produce', async () => {
      const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
      const cluster = fakeCluster(brokers);
      const nodeLatencyTracker = createNodeLatencyTracker();
      const sendMessages = createSendMessages({
        logger: silentLogger,
        cluster: cluster as unknown as Cluster,
        partitioner: cyclingPartitioner,
        eosManager: fakeEosManager(),
        retrier: retrier({ retries: 1, initialRetryTime: 1, maxRetryTime: 5 }),
        nodeLatencyTracker,
      });

      expect(nodeLatencyTracker.latencyFor(1)).toBeUndefined();

      await sendMessages({
        acks: -1,
        timeout: 30_000,
        topicMessages: [{ topic, messages: ninePartitionedMessages() }],
      });

      expect(nodeLatencyTracker.latencyFor(1)).toBeGreaterThanOrEqual(0);
      expect(nodeLatencyTracker.latencyFor(2)).toBeGreaterThanOrEqual(0);
      expect(nodeLatencyTracker.latencyFor(3)).toBeGreaterThanOrEqual(0);
    });

    it('does not record latency for acks: 0, since no response is awaited', async () => {
      const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
      const cluster = fakeCluster(brokers);
      const nodeLatencyTracker = createNodeLatencyTracker();
      const sendMessages = createSendMessages({
        logger: silentLogger,
        cluster: cluster as unknown as Cluster,
        partitioner: cyclingPartitioner,
        eosManager: fakeEosManager(),
        retrier: retrier({ retries: 1, initialRetryTime: 1, maxRetryTime: 5 }),
        nodeLatencyTracker,
      });

      await sendMessages({
        acks: 0,
        timeout: 30_000,
        topicMessages: [{ topic, messages: ninePartitionedMessages() }],
      });

      expect(nodeLatencyTracker.latencyFor(1)).toBeUndefined();
      expect(nodeLatencyTracker.latencyFor(2)).toBeUndefined();
      expect(nodeLatencyTracker.latencyFor(3)).toBeUndefined();
    });
  });

  it('records produce metrics on a successful send', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    const cluster = fakeCluster(brokers);
    const recordProduce = vi.fn();
    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: cluster as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager: fakeEosManager(),
      retrier: retrier({ retries: 1, initialRetryTime: 1, maxRetryTime: 5 }),
      metrics: { recordProduce } as never,
    });

    await sendMessages({
      acks: -1,
      timeout: 30_000,
      topicMessages: [{ topic, messages: [{ key: 'ab', value: 'cd' }] }],
    });

    expect(recordProduce).toHaveBeenCalledWith({ records: 1, bytes: 4, retries: 0 });
  });

  it("keeps a failed batch's sequence numbers across retries instead of handing them to a queued send", async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    const sentSequences: { value: string; firstSequence: number }[] = [];
    brokers[1].produce.mockImplementation(
      (request: { topicData: { partitions: { firstSequence: number; messages: { value: string }[] }[] }[] }) => {
        const entry = request.topicData[0]!.partitions[0]!;
        sentSequences.push({ value: entry.messages[0]!.value, firstSequence: entry.firstSequence });
        if (sentSequences.length === 1) {
          return Promise.reject(new KafkaRequestTimeoutError('Request timed out', { broker: 'h:1' }));
        }
        return Promise.resolve(fakeProduceResponse(topic, 0));
      },
    );

    const eosCluster = {
      refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
      findControllerBroker: vi.fn().mockResolvedValue({
        initProducerId: vi.fn().mockResolvedValue({ producerId: 1000n, producerEpoch: 0 }),
      }),
      brokerPool: { versions: undefined },
    };
    const eosManager = createEosManager({ logger: silentLogger, cluster: eosCluster as unknown as Cluster });
    await eosManager.initProducerId();

    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: fakeCluster(brokers) as unknown as Cluster,
      partitioner: () => 0,
      eosManager,
      retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
    });

    await Promise.all(
      ['first', 'second'].map((value) =>
        sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: [{ value }] }] }),
      ),
    );

    // The timed-out request may already be on the log, so its retry must reuse sequence 0 and the
    // queued send must never be handed that range (the broker would dedupe it away as a duplicate).
    expect(sentSequences).toEqual([
      { value: 'first', firstSequence: 0 },
      { value: 'first', firstSequence: 0 },
      { value: 'second', firstSequence: 1 },
    ]);
    expect(eosManager.getSequence(topic, 0)).toBe(2);
  });

  describe('when a failed send may still have been written', () => {
    const outOfOrderCode = ERROR_CODES.find((entry) => entry.type === 'OUT_OF_ORDER_SEQUENCE_NUMBER')!.code;

    it.each([
      ['a request timeout', () => new KafkaRequestTimeoutError('Request timed out', { broker: 'h:1' })],
      ['OUT_OF_ORDER_SEQUENCE_NUMBER', () => createErrorFromCode(outOfOrderCode, { topic, partition: 0 })],
    ])('bumps the epoch before the next send instead of reusing its sequences after %s', async (_, error) => {
      const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
      const requests: { value: string; producerEpoch: number; firstSequence: number }[] = [];
      brokers[1].produce.mockImplementation(
        (request: {
          producerEpoch: number;
          topicData: { partitions: { firstSequence: number; messages: { value: string }[] }[] }[];
        }) => {
          const entry = request.topicData[0]!.partitions[0]!;
          const value = entry.messages[0]!.value;
          requests.push({ value, producerEpoch: request.producerEpoch, firstSequence: entry.firstSequence });
          return value === 'first' ? Promise.reject(error()) : Promise.resolve(fakeProduceResponse(topic, 0));
        },
      );

      const initProducerId = vi
        .fn()
        .mockResolvedValueOnce({ producerId: 1000n, producerEpoch: 0 })
        .mockResolvedValueOnce({ producerId: 1000n, producerEpoch: 1 });
      const eosCluster = {
        refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
        findControllerBroker: vi.fn().mockResolvedValue({ initProducerId }),
        brokerPool: { versions: undefined },
      };
      const eosManager = createEosManager({ logger: silentLogger, cluster: eosCluster as unknown as Cluster });
      await eosManager.initProducerId();

      const sendMessages = createSendMessages({
        logger: silentLogger,
        cluster: fakeCluster(brokers) as unknown as Cluster,
        partitioner: () => 0,
        eosManager,
        retrier: retrier({ retries: 1, initialRetryTime: 1, maxRetryTime: 5 }),
      });

      await expect(
        sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: [{ value: 'first' }] }] }),
      ).rejects.toBeDefined();
      await sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: [{ value: 'second' }] }] });

      // Reusing sequence 0 under epoch 0 would let the broker drop 'second' as a duplicate of a
      // 'first' that may be on the log; a new epoch restarts sequences without that ambiguity.
      expect(initProducerId).toHaveBeenCalledTimes(2);
      expect(requests.filter(({ value }) => value === 'first')).not.toHaveLength(0);
      for (const request of requests.filter(({ value }) => value === 'first')) {
        expect(request).toEqual({ value: 'first', producerEpoch: 0, firstSequence: 0 });
      }
      expect(requests.filter(({ value }) => value === 'second')).toEqual([
        { value: 'second', producerEpoch: 1, firstSequence: 0 },
      ]);
    });

    it('keeps the sequences of a sibling partition that shared the rejected request', async () => {
      const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
      const code = ERROR_CODES.find((entry) => entry.type === 'MESSAGE_TOO_LARGE')!.code;
      // Partition 0's rejection fails the whole response, but partition 1 in it may have been written.
      brokers[1].produce.mockRejectedValue(createErrorFromCode(code, { topic, partition: 0 }));
      const cluster = fakeCluster(brokers);
      cluster.findLeaderForPartitions.mockReturnValue({ 1: [0, 1] });
      const eosManager = fakeEosManager({ getProducerEpoch: vi.fn().mockReturnValue(0) });

      const sendMessages = createSendMessages({
        logger: silentLogger,
        cluster: cluster as unknown as Cluster,
        partitioner: ({ message }: PartitionerArgs) => Number(message.key),
        eosManager,
        retrier: retrier({ retries: 1, initialRetryTime: 1, maxRetryTime: 5 }),
      });

      await expect(
        sendMessages({
          acks: -1,
          timeout: 30_000,
          topicMessages: [
            {
              topic,
              messages: [
                { key: '0', value: 'a' },
                { key: '1', value: 'b' },
              ],
            },
          ],
        }),
      ).rejects.toMatchObject({ type: 'MESSAGE_TOO_LARGE' });

      expect(eosManager.updateSequence).not.toHaveBeenCalledWith(topic, expect.any(Number), -1);
      expect(eosManager.requestEpochBump).toHaveBeenCalled();
    });
  });
});
