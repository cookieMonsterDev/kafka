import { afterEach, describe, expect, it, vi } from 'vitest';
import type { Cluster, PartitionMetadata } from '../cluster/index';
import { KafkaConnectionError, KafkaDeliveryTimeoutError, KafkaNonRetriableError, KafkaTimeout } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { CONNECTION_STATUS } from '../network/connection-status';
import { COMPRESSION_TYPES } from '../protocol/compression/index';
import { retrier } from '../retry/index';
import type { EosManager } from './eos-manager/index';
import { createMessageProducer, type MessageProducerOptions } from './message-producer';
import type { ProducerAckHookEvent, ProducerSendHookEvent } from './types';

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
    acquirePartitionGates: vi.fn().mockResolvedValue(undefined),
    releasePartitionGates: vi.fn().mockResolvedValue(undefined),
  } as unknown as EosManager;
}

function fakeCluster(broker: ReturnType<typeof fakeBroker>) {
  return {
    addMultipleTargetTopics: vi.fn().mockResolvedValue(undefined),
    refreshMetadata: vi.fn().mockResolvedValue(undefined),
    refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
    findTopicPartitionMetadata: vi.fn().mockReturnValue([fakePartitionMetadata()]),
    findTopicId: vi.fn().mockReturnValue(undefined),
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

  it('flushes independent acks groups in parallel', async () => {
    vi.useFakeTimers();
    const broker = fakeBroker(1);
    let inFlight = 0;
    let maxInFlight = 0;
    broker.produce.mockImplementation(async () => {
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
      await Promise.resolve();
      inFlight -= 1;
      return fakeProduceResponse(topic, 0);
    });
    const producer = createTestProducer(broker, { lingerMs: 50 });

    const first = producer.send({ topic, messages: [{ value: 'a' }], acks: 1 });
    const second = producer.send({ topic, messages: [{ value: 'b' }], acks: -1 });
    await vi.advanceTimersByTimeAsync(50);
    await Promise.all([first, second]);

    expect(broker.produce).toHaveBeenCalledTimes(2);
    expect(maxInFlight).toBe(2);
  });

  it('rejects a linger send whose records exceed bufferMemory', async () => {
    const broker = fakeBroker(1);
    const producer = createTestProducer(broker, { lingerMs: 50, bufferMemory: 4 });

    await expect(producer.send({ topic, messages: [{ value: 'abcde' }] })).rejects.toBeInstanceOf(
      KafkaNonRetriableError,
    );
    expect(broker.produce).not.toHaveBeenCalled();
  });

  it('waits for in-flight produce to free bufferMemory before accepting more records', async () => {
    vi.useFakeTimers();
    const broker = fakeBroker(1);
    let releaseProduce!: (value: unknown) => void;
    broker.produce
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            releaseProduce = resolve;
          }),
      )
      .mockImplementation(() => Promise.resolve(fakeProduceResponse(topic, 0)));
    const producer = createTestProducer(broker, { lingerMs: 50, bufferMemory: 4 });

    const first = producer.send({ topic, messages: [{ value: 'abcd' }] });
    await Promise.resolve();
    expect(broker.produce).not.toHaveBeenCalled();

    const second = producer.send({ topic, messages: [{ value: 'x' }] });
    await vi.waitFor(() => {
      expect(broker.produce).toHaveBeenCalledTimes(1);
    });

    releaseProduce(fakeProduceResponse(topic, 0));
    await first;

    await vi.advanceTimersByTimeAsync(50);
    await second;
    expect(broker.produce).toHaveBeenCalledTimes(2);
  });

  it('rejects when waiting for bufferMemory exceeds the send timeout', async () => {
    vi.useFakeTimers();
    const broker = fakeBroker(1);
    broker.produce.mockImplementation(() => new Promise(() => {}));
    const producer = createTestProducer(broker, { lingerMs: 10_000, bufferMemory: 4 });

    void producer.send({ topic, messages: [{ value: 'abcd' }], timeout: 30_000 });
    await Promise.resolve();

    const second = producer.send({ topic, messages: [{ value: 'x' }], timeout: 50 });
    // eslint-disable-next-line vitest/valid-expect -- attach the matcher before the fake timer fires
    const assertion = expect(second).rejects.toBeInstanceOf(KafkaTimeout);
    await vi.advanceTimersByTimeAsync(50);
    await assertion;
  });

  describe('deliveryTimeoutMs', () => {
    it('rejects with KafkaDeliveryTimeoutError once the deadline elapses, without waiting out the send', async () => {
      vi.useFakeTimers();
      const broker = fakeBroker(1);
      broker.produce.mockImplementation(() => new Promise(() => {})); // never settles
      const producer = createTestProducer(broker, { deliveryTimeoutMs: 500 });

      const sendPromise = producer.send({ topic, messages: [{ value: 'a' }] });
      // eslint-disable-next-line vitest/valid-expect -- attach the matcher before the fake timer fires
      const assertion = expect(sendPromise).rejects.toBeInstanceOf(KafkaDeliveryTimeoutError);
      await vi.advanceTimersByTimeAsync(500);
      await assertion;
    });

    it('does not reject before the deadline while the produce is still pending', async () => {
      vi.useFakeTimers();
      const broker = fakeBroker(1);
      broker.produce.mockImplementation(() => new Promise(() => {}));
      const producer = createTestProducer(broker, { deliveryTimeoutMs: 500 });

      const sendPromise = producer.send({ topic, messages: [{ value: 'a' }] });
      let settled = false;
      sendPromise.then(
        () => (settled = true),
        () => (settled = true),
      );

      await vi.advanceTimersByTimeAsync(499);
      expect(settled).toBe(false);

      await vi.advanceTimersByTimeAsync(1);
      expect(settled).toBe(true);
      await expect(sendPromise).rejects.toBeInstanceOf(KafkaDeliveryTimeoutError);
    });

    it('counts time spent waiting in the linger buffer against the deadline', async () => {
      vi.useFakeTimers();
      const broker = fakeBroker(1);
      const producer = createTestProducer(broker, { lingerMs: 10_000, deliveryTimeoutMs: 500 });

      const sendPromise = producer.send({ topic, messages: [{ value: 'a' }] });
      // eslint-disable-next-line vitest/valid-expect -- attach the matcher before the fake timer fires
      const assertion = expect(sendPromise).rejects.toBeInstanceOf(KafkaDeliveryTimeoutError);
      await vi.advanceTimersByTimeAsync(500);
      await assertion;
      // The linger timer (10s) never got a chance to fire before the 500ms deadline won.
      expect(broker.produce).not.toHaveBeenCalled();
    });

    it('rejects while retries are still scheduled, ahead of the retrier giving up on its own', async () => {
      vi.useFakeTimers();
      const broker = fakeBroker(1);
      broker.produce.mockImplementation(() => Promise.reject(new KafkaConnectionError('connection reset')));
      const producer = createTestProducer(broker, {
        retrier: retrier({ retries: 10, initialRetryTime: 1000, maxRetryTime: 1000, factor: 0, multiplier: 1 }),
        deliveryTimeoutMs: 500,
      });

      const sendPromise = producer.send({ topic, messages: [{ value: 'a' }] });
      // eslint-disable-next-line vitest/valid-expect -- attach the matcher before the fake timer fires
      const assertion = expect(sendPromise).rejects.toBeInstanceOf(KafkaDeliveryTimeoutError);
      await vi.advanceTimersByTimeAsync(500);
      await assertion;
      // Only the first attempt had time to run before the deadline beat the retrier to it.
      expect(broker.produce).toHaveBeenCalledTimes(1);
    });

    it('a non-positive deliveryTimeoutMs disables the deadline', async () => {
      const broker = fakeBroker(1);
      const producer = createTestProducer(broker, { deliveryTimeoutMs: 0 });

      await expect(producer.send({ topic, messages: [{ value: 'a' }] })).resolves.toBeTruthy();
    });

    it('does not reject a send that settles well before the (default) deadline', async () => {
      const broker = fakeBroker(1);
      const producer = createTestProducer(broker);

      await expect(producer.send({ topic, messages: [{ value: 'a' }] })).resolves.toBeTruthy();
    });
  });

  describe('hooks', () => {
    it('fires onSend before dispatch and onAck with metadata after a successful send, in registration order', async () => {
      const broker = fakeBroker(1);
      const order: string[] = [];
      const onSendFirst = vi.fn((_event: ProducerSendHookEvent) => {
        order.push('onSend-1');
      });
      const onSendSecond = vi.fn((_event: ProducerSendHookEvent) => {
        order.push('onSend-2');
      });
      const onAckFirst = vi.fn((_event: ProducerAckHookEvent) => {
        order.push('onAck-1');
      });
      const onAckSecond = vi.fn((_event: ProducerAckHookEvent) => {
        order.push('onAck-2');
      });
      const producer = createTestProducer(broker, {
        hooks: { onSend: [onSendFirst, onSendSecond], onAck: [onAckFirst, onAckSecond] },
      });

      const metadata = await producer.send({ topic, messages: [{ value: 'a' }] });

      expect(order).toEqual(['onSend-1', 'onSend-2', 'onAck-1', 'onAck-2']);
      expect(onSendFirst).toHaveBeenCalledWith(
        expect.objectContaining({
          acks: -1,
          topicMessages: [{ topic, messages: [{ value: 'a' }] }],
        }),
      );
      expect(onAckFirst).toHaveBeenCalledWith(expect.objectContaining({ metadata }));
      const ackEvent = onAckFirst.mock.calls[0]?.[0];
      expect(ackEvent?.error).toBeUndefined();
    });

    it('fires onAck with the error, not metadata, when the send fails - and still rejects', async () => {
      const broker = fakeBroker(1);
      const sendError = new KafkaNonRetriableError('boom');
      broker.produce.mockImplementation(() => Promise.reject(sendError));
      const onAck = vi.fn((_event: ProducerAckHookEvent) => undefined);
      const producer = createTestProducer(broker, { hooks: { onAck: [onAck] } });

      await expect(producer.send({ topic, messages: [{ value: 'a' }] })).rejects.toThrow(sendError);

      expect(onAck).toHaveBeenCalledWith(expect.objectContaining({ error: sendError }));
      const event = onAck.mock.calls[0]?.[0];
      expect(event?.metadata).toBeUndefined();
    });

    it('does not fail the send when an onSend or onAck hook throws', async () => {
      const broker = fakeBroker(1);
      const throwingOnSend = vi.fn(() => {
        throw new Error('onSend boom');
      });
      const throwingOnAck = vi.fn(() => {
        throw new Error('onAck boom');
      });
      const producer = createTestProducer(broker, {
        hooks: { onSend: [throwingOnSend], onAck: [throwingOnAck] },
      });

      await expect(producer.send({ topic, messages: [{ value: 'a' }] })).resolves.toBeTruthy();
      expect(throwingOnSend).toHaveBeenCalledTimes(1);
      expect(throwingOnAck).toHaveBeenCalledTimes(1);
    });

    it('does not fail send() when a throwing onAck hook runs on the failure path', async () => {
      const broker = fakeBroker(1);
      const sendError = new KafkaNonRetriableError('boom');
      broker.produce.mockImplementation(() => Promise.reject(sendError));
      const throwingOnAck = vi.fn(() => {
        throw new Error('onAck boom');
      });
      const producer = createTestProducer(broker, { hooks: { onAck: [throwingOnAck] } });

      await expect(producer.send({ topic, messages: [{ value: 'a' }] })).rejects.toThrow(sendError);
      expect(throwingOnAck).toHaveBeenCalledTimes(1);
    });
  });
});
