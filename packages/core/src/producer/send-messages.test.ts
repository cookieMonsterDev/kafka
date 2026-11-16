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
      hasLoadedTargetTopics: vi.fn().mockReturnValue(false),
      needsMetadataRefresh: vi.fn().mockReturnValue(true),
      connectedBroker: vi.fn().mockReturnValue(undefined),
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

  it('skips the partition gates when no producer id is initialised', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    const eosManager = fakeEosManager();
    const sendMessages = createSendMessages({
      logger: silentLogger,
      cluster: fakeCluster(brokers) as unknown as Cluster,
      partitioner: cyclingPartitioner,
      eosManager,
      retrier: retrier({ retries: 0 }),
    });

    await sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: ninePartitionedMessages() }] });

    expect(eosManager.acquirePartitionGates).not.toHaveBeenCalled();
    expect(eosManager.releasePartitionGates).not.toHaveBeenCalled();
  });

  it('retrieves sequence information from the transaction manager and updates it', async () => {
    const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
    const cluster = fakeCluster(brokers);
    const eosManager = fakeEosManager({
      getSequence: vi.fn().mockReturnValue(5),
      isInitialized: vi.fn().mockReturnValue(true),
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
    const eosManager = fakeEosManager({
      getProducerEpoch: vi.fn().mockReturnValue(0),
      isInitialized: vi.fn().mockReturnValue(true),
    });

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
    const eosManager = fakeEosManager({
      getProducerEpoch: vi.fn(() => epoch),
      isInitialized: vi.fn().mockReturnValue(true),
    });

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

    it("does not bump the epoch inside another send's retries, which may resend a written batch", async () => {
      const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
      const initProducerId = vi
        .fn()
        .mockResolvedValueOnce({ producerId: 1000n, producerEpoch: 0 })
        .mockResolvedValue({ producerId: 1000n, producerEpoch: 1 });
      const eosCluster = {
        refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
        findControllerBroker: vi.fn().mockResolvedValue({ initProducerId }),
        brokerPool: { versions: undefined },
      };
      const eosManager = createEosManager({ logger: silentLogger, cluster: eosCluster as unknown as Cluster });
      await eosManager.initProducerId();

      const requests: { producerEpoch: number; firstSequence: number }[] = [];
      brokers[1].produce.mockImplementation(
        (request: { producerEpoch: number; topicData: { partitions: { firstSequence: number }[] }[] }) => {
          requests.push({
            producerEpoch: request.producerEpoch,
            firstSequence: request.topicData[0]!.partitions[0]!.firstSequence,
          });
          if (requests.length > 1) return Promise.resolve(fakeProduceResponse(topic, 0));
          // Another send gives up on a possibly-written batch while this one is in flight.
          eosManager.requestEpochBump();
          return Promise.reject(new KafkaRequestTimeoutError('Request timed out', { broker: 'h:1' }));
        },
      );

      const sendMessages = createSendMessages({
        logger: silentLogger,
        cluster: fakeCluster(brokers) as unknown as Cluster,
        partitioner: () => 0,
        eosManager,
        retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
      });
      await sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: [{ value: 'a' }] }] });

      // The retry must reuse its sequence under the same epoch so the broker can dedupe it.
      expect(requests).toEqual([
        { producerEpoch: 0, firstSequence: 0 },
        { producerEpoch: 0, firstSequence: 0 },
      ]);
      expect(initProducerId).toHaveBeenCalledTimes(1);
    });

    it('keeps the sequences of a sibling partition that shared the rejected request', async () => {
      const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
      const code = ERROR_CODES.find((entry) => entry.type === 'MESSAGE_TOO_LARGE')!.code;
      // Partition 0's rejection fails the whole response, but partition 1 in it may have been written.
      brokers[1].produce.mockRejectedValue(createErrorFromCode(code, { topic, partition: 0 }));
      const cluster = fakeCluster(brokers);
      cluster.findLeaderForPartitions.mockReturnValue({ 1: [0, 1] });
      const eosManager = fakeEosManager({
        getProducerEpoch: vi.fn().mockReturnValue(0),
        isInitialized: vi.fn().mockReturnValue(true),
      });

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

  describe('when the broker no longer knows the producer id', () => {
    const unknownProducerIdCode = ERROR_CODES.find((entry) => entry.type === 'UNKNOWN_PRODUCER_ID')!.code;

    interface ProduceRequest {
      producerEpoch: number;
      topicData: { partitions: { partition: number; firstSequence: number; messages: { value: string }[] }[] }[];
    }

    /** A partition log that, like the broker, drops a batch whose epoch and first sequence it already appended. */
    function idempotentLog() {
      const values = new Map<number, string[]>();
      const appended = new Set<string>();
      return {
        append({ producerEpoch, topicData }: ProduceRequest): void {
          for (const { partition, firstSequence, messages } of topicData[0]!.partitions) {
            const batch = `${partition}:${producerEpoch}:${firstSequence}`;
            if (appended.has(batch)) continue;
            appended.add(batch);
            values.set(partition, [...(values.get(partition) ?? []), ...messages.map(({ value }) => value)]);
          }
        },
        values: (partition: number) => values.get(partition) ?? [],
      };
    }

    async function initializedEosManager() {
      let epoch = -1;
      const initProducerId = vi.fn(() => Promise.resolve({ producerId: 1000n, producerEpoch: (epoch += 1) }));
      const eosCluster = {
        refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
        findControllerBroker: vi.fn().mockResolvedValue({ initProducerId }),
        brokerPool: { versions: undefined },
      };
      const eosManager = createEosManager({ logger: silentLogger, cluster: eosCluster as unknown as Cluster });
      await eosManager.initProducerId();
      return { eosManager, initProducerId };
    }

    function delay(ms: number): Promise<void> {
      return new Promise((resolve) => setTimeout(resolve, ms));
    }

    it("waits for another send's retries before bumping, so that send's written batch is not resent", async () => {
      const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
      const log = idempotentLog();
      const { eosManager, initProducerId } = await initializedEosManager();

      // Partition 0 lost its producer id; partition 1's first request is written but its response is lost.
      let timeOutPartition1: () => void = () => {};
      brokers[1].produce.mockImplementation((request: ProduceRequest) => {
        if (request.producerEpoch === 0) return Promise.reject(createErrorFromCode(unknownProducerIdCode));
        log.append(request);
        return Promise.resolve(fakeProduceResponse(topic, 0));
      });
      brokers[2].produce
        .mockImplementationOnce(
          (request: ProduceRequest) =>
            new Promise((_, reject) => {
              timeOutPartition1 = () => {
                log.append(request);
                reject(new KafkaRequestTimeoutError('Request timed out', { broker: 'h:2' }));
              };
            }),
        )
        .mockImplementation((request: ProduceRequest) => {
          log.append(request);
          return Promise.resolve(fakeProduceResponse(topic, 1));
        });

      const sendMessages = createSendMessages({
        logger: silentLogger,
        cluster: fakeCluster(brokers) as unknown as Cluster,
        partitioner: ({ message }: PartitionerArgs) => Number(message.key),
        eosManager,
        retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
      });

      const sends = Promise.all([
        sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: [{ key: '0', value: 'a' }] }] }),
        sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: [{ key: '1', value: 'b' }] }] }),
      ]);
      // Give the producer-id recovery time to run while partition 1's request is still in flight.
      await delay(20);
      timeOutPartition1();
      await sends;

      expect(log.values(0)).toEqual(['a']);
      expect(log.values(1)).toEqual(['b']);
      expect(initProducerId).toHaveBeenCalledTimes(2);
      expect(eosManager.getProducerEpoch()).toBe(1);
    });

    it('re-initializes the producer id once when several sends lose it together', async () => {
      const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
      const log = idempotentLog();
      const { eosManager, initProducerId } = await initializedEosManager();
      for (const [nodeId, broker] of Object.entries(brokers)) {
        broker.produce.mockImplementation((request: ProduceRequest) => {
          if (request.producerEpoch === 0) return Promise.reject(createErrorFromCode(unknownProducerIdCode));
          log.append(request);
          return Promise.resolve(fakeProduceResponse(topic, Number(nodeId) - 1));
        });
      }

      const sendMessages = createSendMessages({
        logger: silentLogger,
        cluster: fakeCluster(brokers) as unknown as Cluster,
        partitioner: ({ message }: PartitionerArgs) => Number(message.key),
        eosManager,
        retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
      });

      await Promise.all(
        ['0', '1', '2'].map((key) =>
          sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: [{ key, value: key }] }] }),
        ),
      );

      expect(initProducerId).toHaveBeenCalledTimes(2);
      expect([log.values(0), log.values(1), log.values(2)]).toEqual([['0'], ['1'], ['2']]);
    });

    it('fails a transactional send so the transaction aborts, bumping the epoch then', async () => {
      const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
      brokers[1].produce.mockRejectedValue(createErrorFromCode(unknownProducerIdCode));
      let epoch = -1;
      const coordinator = {
        initProducerId: vi.fn(() => Promise.resolve({ producerId: 1000n, producerEpoch: (epoch += 1) })),
        addPartitionsToTxn: vi.fn().mockResolvedValue(undefined),
        endTxn: vi.fn().mockResolvedValue(undefined),
      };
      const eosManager = createEosManager({
        logger: silentLogger,
        cluster: {
          refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
          findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
          brokerPool: { versions: undefined },
        } as unknown as Cluster,
        transactional: true,
        transactionalId: 'transactional-id',
      });
      await eosManager.initProducerId();
      eosManager.beginTransaction();

      const sendMessages = createSendMessages({
        logger: silentLogger,
        cluster: fakeCluster(brokers) as unknown as Cluster,
        partitioner: () => 0,
        eosManager,
        retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
      });

      await expect(
        sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: [{ value: 'a' }] }] }),
      ).rejects.toMatchObject({ type: 'UNKNOWN_PRODUCER_ID' });
      expect(brokers[1].produce).toHaveBeenCalledTimes(1);

      await eosManager.abort();
      expect(coordinator.initProducerId).toHaveBeenCalledTimes(2);
      expect(eosManager.getProducerEpoch()).toBe(1);
    });
  });

  describe('when metadata is fresh and the leaders are connected', () => {
    function fastPathCluster(
      brokers: Record<number, ReturnType<typeof fakeBroker>>,
      disconnectedNodeIds: readonly string[] = [],
    ) {
      const cluster = fakeCluster(brokers);
      cluster.hasLoadedTargetTopics.mockReturnValue(true);
      cluster.needsMetadataRefresh.mockReturnValue(false);
      cluster.connectedBroker.mockImplementation((nodeId: string) =>
        disconnectedNodeIds.includes(nodeId) ? undefined : brokers[Number(nodeId)],
      );
      return cluster;
    }

    function createFastPathSend(cluster: ReturnType<typeof fakeCluster>, eosManager: EosManager = fakeEosManager()) {
      return createSendMessages({
        logger: silentLogger,
        cluster: cluster as unknown as Cluster,
        partitioner: cyclingPartitioner,
        eosManager,
        retrier: retrier({ retries: 5, initialRetryTime: 1, maxRetryTime: 5 }),
      });
    }

    it('produces without the async target-topic, metadata or broker lookups', async () => {
      const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
      const cluster = fastPathCluster(brokers);

      const response = await createFastPathSend(cluster)({
        acks: -1,
        timeout: 30_000,
        topicMessages: [{ topic, messages: ninePartitionedMessages() }],
      });

      expect(cluster.hasLoadedTargetTopics).toHaveBeenCalledWith([topic]);
      expect(cluster.addMultipleTargetTopics).not.toHaveBeenCalled();
      expect(cluster.refreshMetadataIfNecessary).not.toHaveBeenCalled();
      expect(cluster.findBroker).not.toHaveBeenCalled();
      expect(response.map(({ partition }) => partition)).toEqual([0, 1, 2]);
    });

    it('awaits addMultipleTargetTopics when a topic is not yet targeted', async () => {
      const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
      const cluster = fastPathCluster(brokers);
      cluster.hasLoadedTargetTopics.mockReturnValue(false);

      await createFastPathSend(cluster)({
        acks: -1,
        timeout: 30_000,
        topicMessages: [{ topic, messages: ninePartitionedMessages() }],
      });

      expect(cluster.addMultipleTargetTopics).toHaveBeenCalledExactlyOnceWith([topic]);
      expect(cluster.refreshMetadataIfNecessary).not.toHaveBeenCalled();
    });

    it('awaits refreshMetadataIfNecessary when the metadata is stale', async () => {
      const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
      const cluster = fastPathCluster(brokers);
      cluster.needsMetadataRefresh.mockReturnValue(true);

      await createFastPathSend(cluster)({
        acks: -1,
        timeout: 30_000,
        topicMessages: [{ topic, messages: ninePartitionedMessages() }],
      });

      expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalledOnce();
      expect(cluster.addMultipleTargetTopics).not.toHaveBeenCalled();
    });

    it('falls back to findBroker for a leader that is not connected', async () => {
      const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
      const cluster = fastPathCluster(brokers, ['2']);

      await createFastPathSend(cluster)({
        acks: -1,
        timeout: 30_000,
        topicMessages: [{ topic, messages: ninePartitionedMessages() }],
      });

      expect(cluster.findBroker).toHaveBeenCalledExactlyOnceWith({ nodeId: '2' });
      expect(brokers[2].produce).toHaveBeenCalledOnce();
    });

    it('resolves with the record metadata when its only request succeeds', async () => {
      const brokers = { 1: fakeBroker(1) };

      const response = await createFastPathSend(fastPathCluster(brokers))({
        acks: -1,
        timeout: 30_000,
        topicMessages: [{ topic, messages: [{ value: 'v0', key: '0' }] }],
      });

      expect(brokers[1].produce).toHaveBeenCalledOnce();
      expect(response).toEqual([
        { topicName: topic, partition: 0, errorCode: 0, baseOffset: 0n, logAppendTime: -1n, logStartOffset: 0n },
      ]);
    });

    it('rejects with the broker error when its only request fails', async () => {
      const brokers = { 1: fakeBroker(1) };
      const code = ERROR_CODES.find((entry) => entry.type === 'MESSAGE_TOO_LARGE')!.code;
      brokers[1].produce.mockRejectedValue(createErrorFromCode(code, { topic, partition: 0 }));
      const cluster = fastPathCluster(brokers);

      await expect(
        createFastPathSend(cluster)({
          acks: -1,
          timeout: 30_000,
          topicMessages: [{ topic, messages: [{ value: 'v0', key: '0' }] }],
        }),
      ).rejects.toMatchObject({ type: 'MESSAGE_TOO_LARGE' });
    });

    it('still tracks sequences for an idempotent producer', async () => {
      const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
      const eosManager = fakeEosManager({
        getSequence: vi.fn().mockReturnValue(7),
        isInitialized: vi.fn().mockReturnValue(true),
      });

      await createFastPathSend(
        fastPathCluster(brokers),
        eosManager,
      )({
        acks: -1,
        timeout: 30_000,
        topicMessages: [{ topic, messages: ninePartitionedMessages() }],
      });

      for (const nodeId of [1, 2, 3] as const) {
        const request = brokers[nodeId].produce.mock.calls[0]?.[0];
        expect(request.topicData[0]?.partitions[0]).toEqual(expect.objectContaining({ firstSequence: 7 }));
      }
      expect(eosManager.updateSequence).toHaveBeenCalledWith(topic, 0, 3);
      expect(eosManager.updateSequence).toHaveBeenCalledWith(topic, 1, 3);
      expect(eosManager.updateSequence).toHaveBeenCalledWith(topic, 2, 3);
    });

    it('skips sequence tracking when no producer id is initialised', async () => {
      const brokers = { 1: fakeBroker(1), 2: fakeBroker(2), 3: fakeBroker(3) };
      const eosManager = fakeEosManager();

      await createFastPathSend(
        fastPathCluster(brokers),
        eosManager,
      )({
        acks: -1,
        timeout: 30_000,
        topicMessages: [{ topic, messages: ninePartitionedMessages() }],
      });

      const request = brokers[1].produce.mock.calls[0]?.[0];
      expect(request.topicData[0]?.partitions[0]?.firstSequence).toBeUndefined();
      expect(eosManager.getSequence).not.toHaveBeenCalled();
      expect(eosManager.updateSequence).not.toHaveBeenCalled();
    });
  });
});
