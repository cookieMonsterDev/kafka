import { afterEach, describe, expect, it, vi } from 'vitest';
import type { Cluster, PartitionMetadata } from '../cluster/index';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { CONNECTION_STATUS } from '../network/connection-status';
import { COMPRESSION_TYPES } from '../protocol/compression/index';
import { retrier } from '../retry/index';
import type { EosManager } from './eos-manager/index';
import { createMessageProducer, type MessageProducerOptions } from './message-producer';

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
  return { nodeId, produce: vi.fn().mockImplementation(() => Promise.resolve(fakeProduceResponse(topic, 0))) };
}

function fakePartitionMetadata(): PartitionMetadata {
  return {
    partitionErrorCode: 0,
    partitionId: 0,
    leader: 1,
    replicas: [1],
    isr: [1],
    offlineReplicas: [],
  };
}

function fakeEosManager() {
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
  } as unknown as EosManager;
}

function fakeCluster(broker: ReturnType<typeof fakeBroker>) {
  return {
    addMultipleTargetTopics: vi.fn().mockResolvedValue(undefined),
    refreshMetadata: vi.fn().mockResolvedValue(undefined),
    refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
    findTopicPartitionMetadata: vi.fn().mockReturnValue([fakePartitionMetadata()]),
    findLeaderForPartitions: vi.fn().mockReturnValue({ 1: [0] }),
    findBroker: vi.fn().mockResolvedValue(broker),
    removeBroker: vi.fn(),
    connect: vi.fn().mockResolvedValue(undefined),
    targetTopics: new Set<string>(),
    isConnected: vi.fn().mockReturnValue(true),
    brokerPool: { versions: undefined },
  };
}

function producedMessages(broker: ReturnType<typeof fakeBroker>) {
  const arg = broker.produce.mock.calls[0]?.[0] as {
    topicData: Array<{ partitions: Array<{ messages: Array<{ value: unknown }> }> }>;
  };
  return arg.topicData[0]?.partitions[0]?.messages.map((message) => message.value) ?? [];
}

function createTestProducer(broker: ReturnType<typeof fakeBroker>, overrides: Partial<MessageProducerOptions> = {}) {
  return createMessageProducer({
    logger: silentLogger,
    cluster: fakeCluster(broker) as unknown as Cluster,
    partitioner: () => 0,
    eosManager: fakeEosManager(),
    idempotent: false,
    retrier: retrier({ retries: 0 }),
    getConnectionStatus: () => CONNECTION_STATUS.CONNECTED,
    ...overrides,
  });
}

describe('producer/messageProducer', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('sends immediately when lingerMs is 0', async () => {
    vi.useFakeTimers();
    const broker = fakeBroker(1);
    const producer = createTestProducer(broker);

    const sendPromise = producer.send({ topic, messages: [{ value: 'a' }] });
    await sendPromise;

    expect(broker.produce).toHaveBeenCalledTimes(1);
    expect(broker.produce).toHaveBeenCalledWith(expect.objectContaining({ acks: -1 }));
  });

  it('does not send until the linger timer fires', async () => {
    vi.useFakeTimers();
    const broker = fakeBroker(1);
    const producer = createTestProducer(broker, { lingerMs: 50 });

    const sendPromise = producer.send({ topic, messages: [{ value: 'a' }] });
    await Promise.resolve();
    expect(broker.produce).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(49);
    expect(broker.produce).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(1);
    await sendPromise;
    expect(broker.produce).toHaveBeenCalledTimes(1);
  });

  it('coalesces two sends within linger into one Produce when acks and compression match', async () => {
    vi.useFakeTimers();
    const broker = fakeBroker(1);
    const producer = createTestProducer(broker, { lingerMs: 50 });

    const first = producer.send({ topic, messages: [{ value: 'a' }] });
    const second = producer.send({ topic, messages: [{ value: 'b' }] });
    await Promise.resolve();
    expect(broker.produce).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(50);
    await Promise.all([first, second]);

    expect(broker.produce).toHaveBeenCalledTimes(1);
    expect(producedMessages(broker)).toEqual(['a', 'b']);
  });

  it('does not coalesce sends that differ in acks', async () => {
    vi.useFakeTimers();
    const broker = fakeBroker(1);
    const producer = createTestProducer(broker, { lingerMs: 50 });

    const first = producer.send({ topic, messages: [{ value: 'a' }], acks: 1 });
    const second = producer.send({ topic, messages: [{ value: 'b' }], acks: -1 });
    await vi.advanceTimersByTimeAsync(50);
    await Promise.all([first, second]);

    expect(broker.produce).toHaveBeenCalledTimes(2);
    expect(broker.produce.mock.calls.map((call) => call[0].acks).sort()).toEqual([-1, 1]);
  });

  it('uses producer default acks when send omits acks, and lets the call override', async () => {
    const broker = fakeBroker(1);
    const producer = createTestProducer(broker, { defaultAcks: 1 });

    await producer.send({ topic, messages: [{ value: 'a' }] });
    expect(broker.produce).toHaveBeenCalledWith(expect.objectContaining({ acks: 1 }));

    await producer.send({ topic, messages: [{ value: 'b' }], acks: -1 });
    expect(broker.produce).toHaveBeenLastCalledWith(expect.objectContaining({ acks: -1 }));
  });

  it('uses producer default compression when send omits compression, and lets the call override', async () => {
    const broker = fakeBroker(1);
    const producer = createTestProducer(broker, { defaultCompression: COMPRESSION_TYPES.GZIP });

    await producer.send({ topic, messages: [{ value: 'a' }] });
    expect(broker.produce).toHaveBeenCalledWith(expect.objectContaining({ compression: COMPRESSION_TYPES.GZIP }));

    await producer.send({ topic, messages: [{ value: 'b' }], compression: COMPRESSION_TYPES.None });
    expect(broker.produce).toHaveBeenLastCalledWith(expect.objectContaining({ compression: COMPRESSION_TYPES.None }));
  });

  it('rejects an idempotent send when resolved acks is not -1', async () => {
    const broker = fakeBroker(1);
    const producer = createTestProducer(broker, { idempotent: true, defaultAcks: 1 });

    await expect(producer.send({ topic, messages: [{ value: 'a' }] })).rejects.toThrow(
      /idempotent producer's EoS guarantees/,
    );
    expect(broker.produce).not.toHaveBeenCalled();
  });

  it('flushes when buffered bytes reach batchSize before linger expires', async () => {
    vi.useFakeTimers();
    const broker = fakeBroker(1);
    const producer = createTestProducer(broker, { lingerMs: 10_000, batchSize: 4 });

    const sendPromise = producer.send({ topic, messages: [{ value: 'abcd' }] });
    await sendPromise;

    expect(broker.produce).toHaveBeenCalledTimes(1);
  });

  it('flush sends linger-buffered records without waiting for the timer', async () => {
    vi.useFakeTimers();
    const broker = fakeBroker(1);
    const producer = createTestProducer(broker, { lingerMs: 10_000 });

    const sendPromise = producer.send({ topic, messages: [{ value: 'a' }] });
    await Promise.resolve();
    expect(broker.produce).not.toHaveBeenCalled();

    await producer.flush();
    await sendPromise;
    expect(broker.produce).toHaveBeenCalledTimes(1);
  });
});
