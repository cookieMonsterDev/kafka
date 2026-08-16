import { describe, expect, it, vi } from 'vitest';
import type { Cluster, PartitionMetadata } from '../cluster/index';
import { KafkaRequestTimeoutError } from '../errors';
import { createErrorFromCode, ERROR_CODES } from '../protocol/error-codes';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { retrier } from '../retry/index';
import type { EosManager } from './eos-manager/index';
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
    addPartitionsToTransaction: vi.fn().mockResolvedValue(undefined),
    acquireBrokerLock: vi.fn().mockResolvedValue(undefined),
    releaseBrokerLock: vi.fn().mockResolvedValue(undefined),
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
      findTopicPartitionMetadata: vi.fn().mockReturnValue(partitionMetadata),
      findLeaderForPartitions: vi.fn().mockReturnValue(partitionsPerLeader),
      findBroker: vi
        .fn()
        .mockImplementation(({ nodeId }: { nodeId: string }) => Promise.resolve(brokers[Number(nodeId)])),
      removeBroker: vi.fn(),
      connect: vi.fn().mockResolvedValue(undefined),
      targetTopics: new Set<string>(),
      isConnected: vi.fn().mockReturnValue(true),
    };
  }

  it('retries produce to every broker after a failed send so stale leader acks are not kept', async () => {
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

    expect(brokers[1].produce).toHaveBeenCalledTimes(3);
    expect(brokers[2].produce).toHaveBeenCalledTimes(3);
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

  it('does not keep a sibling broker error-0 ack after NOT_LEADER_OR_FOLLOWER', async () => {
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

    await sendMessages({ acks: -1, timeout: 30_000, topicMessages: [{ topic, messages: ninePartitionedMessages() }] });

    expect(cluster.refreshMetadata).toHaveBeenCalled();
    expect(brokers[1].produce.mock.calls.length).toBeGreaterThan(1);
    expect(brokers[2].produce.mock.calls.length).toBeGreaterThan(1);
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
});
