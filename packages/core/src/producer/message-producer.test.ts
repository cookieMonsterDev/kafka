import { afterEach, describe, expect, it, vi } from 'vitest';
import type { Cluster, PartitionMetadata } from '../cluster/index';
import {
  KafkaConnectionError,
  KafkaDeliveryTimeoutError,
  KafkaMessageTooLargeError,
  KafkaNonRetriableError,
} from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { CONNECTION_STATUS } from '../network/connection-status';
import { COMPRESSION_TYPES } from '../protocol/compression/index';
import { retrier } from '../retry/index';
import { createIdempotenceLedger, type IdempotenceLedger, type ProducerIdentity } from './idempotence-ledger';
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

/** Sequences and gates run on a real ledger with an in-memory InitProducerId. */
function inMemoryLedger(): IdempotenceLedger {
  return createIdempotenceLedger({
    logger: silentLogger,
    initProducerId: (current: ProducerIdentity) =>
      Promise.resolve({ producerId: 1000n, producerEpoch: current.producerEpoch + 1 }),
  });
}

/** An idempotent producer's ledger, its producer id already initialized. */
async function idempotentLedger(): Promise<IdempotenceLedger> {
  const ledger = inMemoryLedger();
  await ledger.initProducerId();
  return ledger;
}

function sentFirstSequences(broker: ReturnType<typeof fakeBroker>): (number | undefined)[] {
  return broker.produce.mock.calls.map((call) => {
    const request = call[0] as { topicData: { partitions: { firstSequence?: number }[] }[] };
    return request.topicData[0]?.partitions[0]?.firstSequence;
  });
}

function fakeCluster(broker: ReturnType<typeof fakeBroker>) {
  return {
    addMultipleTargetTopics: vi.fn().mockResolvedValue(undefined),
    hasLoadedTargetTopics: vi.fn().mockReturnValue(false),
    needsMetadataRefresh: vi.fn().mockReturnValue(true),
    connectedBroker: vi.fn().mockReturnValue(undefined),
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

function producedMessages(broker: ReturnType<typeof fakeBroker>, callIndex = 0) {
  const arg = broker.produce.mock.calls[callIndex]?.[0] as {
    topicData: Array<{ partitions: Array<{ messages: Array<{ value: unknown }> }> }>;
  };
  return arg.topicData[0]?.partitions[0]?.messages.map((message) => message.value) ?? [];
}

function createTestProducer(broker: ReturnType<typeof fakeBroker>, overrides: Partial<MessageProducerOptions> = {}) {
  return createMessageProducer({
    logger: silentLogger,
    cluster: fakeCluster(broker) as unknown as Cluster,
    partitioner: () => 0,
    ledger: inMemoryLedger(),
    idempotent: false,
    retrier: retrier({ retries: 0 }),
    getConnectionStatus: () => CONNECTION_STATUS.CONNECTED,
    ...overrides,
  });
}

describe('producer/messageProducer', () => {
  afterEach(() => {
    // Restore spies before the fake timers, or restoring a spy would reinstall a fake setTimeout.
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it('sends immediately when lingerMs is 0', async () => {
    vi.useFakeTimers();
    const broker = fakeBroker(1);
    const producer = createTestProducer(broker, { lingerMs: 0 });

    const sendPromise = producer.send({ topic, messages: [{ value: 'a' }] });
    await sendPromise;

    expect(broker.produce).toHaveBeenCalledTimes(1);
    expect(broker.produce).toHaveBeenCalledWith(expect.objectContaining({ acks: -1 }));
  });

  it('defaults lingerMs to 5 so a send waits for the linger timer', async () => {
    vi.useFakeTimers();
    const broker = fakeBroker(1);
    const producer = createTestProducer(broker);

    const sendPromise = producer.send({ topic, messages: [{ value: 'a' }] });
    await Promise.resolve();
    expect(broker.produce).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(4);
    expect(broker.produce).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(1);
    await sendPromise;
    expect(broker.produce).toHaveBeenCalledTimes(1);
  });

  it('defaults batchSize to 16384 and flushes when buffered bytes reach it', async () => {
    vi.useFakeTimers();
    const broker = fakeBroker(1);
    const producer = createTestProducer(broker, { lingerMs: 10_000 });

    const sendPromise = producer.send({ topic, messages: [{ value: 'a'.repeat(16_384) }] });
    await sendPromise;

    expect(broker.produce).toHaveBeenCalledTimes(1);
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

  it('uses producer default compressionLevel when send omits it, and lets the call override', async () => {
    const broker = fakeBroker(1);
    const producer = createTestProducer(broker, {
      defaultCompression: COMPRESSION_TYPES.GZIP,
      defaultCompressionLevel: 9,
    });

    await producer.send({ topic, messages: [{ value: 'a' }] });
    expect(broker.produce).toHaveBeenCalledWith(expect.objectContaining({ compressionLevel: 9 }));

    await producer.send({ topic, messages: [{ value: 'b' }], compressionLevel: 1 });
    expect(broker.produce).toHaveBeenLastCalledWith(expect.objectContaining({ compressionLevel: 1 }));
  });

  it('rejects an idempotent send when resolved acks is not -1', async () => {
    const broker = fakeBroker(1);
    const producer = createTestProducer(broker, { idempotent: true, defaultAcks: 1 });

    await expect(producer.send({ topic, messages: [{ value: 'a' }] })).rejects.toThrow(
      /idempotent producer's EoS guarantees/,
    );
    expect(broker.produce).not.toHaveBeenCalled();
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

  describe('same-turn sends with lingerMs 0', () => {
    it('sends calls made in the same turn as one Produce request', async () => {
      const broker = fakeBroker(1);
      const producer = createTestProducer(broker, { lingerMs: 0 });

      const first = producer.send({ topic, messages: [{ value: 'a' }] });
      const second = producer.send({ topic, messages: [{ value: 'b' }] });
      expect(broker.produce).not.toHaveBeenCalled();
      await Promise.all([first, second]);

      expect(broker.produce).toHaveBeenCalledTimes(1);
      expect(producedMessages(broker, 0)).toEqual(['a', 'b']);
    });

    it('sends awaited calls separately, sharing one deadline timer', async () => {
      vi.useFakeTimers();
      const setTimeoutSpy = vi.spyOn(globalThis, 'setTimeout');
      const broker = fakeBroker(1);
      const producer = createTestProducer(broker, { lingerMs: 0, deliveryTimeoutMs: 500 });

      for (const value of ['a', 'b']) {
        const sendPromise = producer.send({ topic, messages: [{ value }] });
        await Promise.resolve();
        expect(vi.getTimerCount()).toBe(1);
        await sendPromise;
      }

      expect(setTimeoutSpy).toHaveBeenCalledOnce();
      expect(broker.produce).toHaveBeenCalledTimes(2);
      expect(producedMessages(broker, 0)).toEqual(['a']);
      expect(producedMessages(broker, 1)).toEqual(['b']);
    });

    it('shares one deadline timer for a shared request and rejects every caller in it at the deadline', async () => {
      vi.useFakeTimers();
      const broker = fakeBroker(1);
      broker.produce.mockImplementation(() => new Promise(() => {}));
      const producer = createTestProducer(broker, { lingerMs: 0, deliveryTimeoutMs: 500 });

      const sends = [
        producer.send({ topic, messages: [{ value: 'a' }] }),
        producer.send({ topic, messages: [{ value: 'b' }] }),
      ];
      // Attach the matchers before the fake timer fires.
      const assertions = sends.map((sendPromise) =>
        expect(sendPromise).rejects.toBeInstanceOf(KafkaDeliveryTimeoutError),
      );
      await vi.advanceTimersByTimeAsync(0);
      expect(broker.produce).toHaveBeenCalledTimes(1);
      expect(vi.getTimerCount()).toBe(1);

      await vi.advanceTimersByTimeAsync(500);
      await Promise.all(assertions);
    });

    it('assigns one sequence range to same-turn calls of an idempotent producer', async () => {
      const broker = fakeBroker(1);
      const ledger = await idempotentLedger();
      const producer = createTestProducer(broker, { lingerMs: 0, ledger, idempotent: true });

      await Promise.all([
        producer.send({ topic, messages: [{ value: 'a' }] }),
        producer.send({ topic, messages: [{ value: 'b' }, { value: 'c' }] }),
      ]);
      await producer.send({ topic, messages: [{ value: 'd' }] });

      expect(sentFirstSequences(broker)).toEqual([0, 3]);
    });

    it('adds the partitions of same-turn transactional calls to the transaction once', async () => {
      const broker = fakeBroker(1);
      const transaction = {
        transactionalId: 'transactional-id',
        enrollPartitions: vi.fn((): Promise<void> | undefined => undefined),
        sendFailed: vi.fn(),
      };
      const producer = createTestProducer(broker, {
        lingerMs: 0,
        ledger: await idempotentLedger(),
        transaction,
        idempotent: true,
      });

      await Promise.all([
        producer.send({ topic, messages: [{ value: 'a' }] }),
        producer.send({ topic, messages: [{ value: 'b' }] }),
      ]);

      expect(broker.produce).toHaveBeenCalledTimes(1);
      expect(broker.produce).toHaveBeenCalledWith(expect.objectContaining({ transactionalId: 'transactional-id' }));
      expect(transaction.enrollPartitions).toHaveBeenCalledTimes(1);
    });
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

    it('stops retrying and releases the partition gates once the deadline elapses', async () => {
      vi.useFakeTimers();
      const broker = fakeBroker(1);
      broker.produce.mockImplementation(() => Promise.reject(new KafkaConnectionError('connection reset')));
      const producer = createTestProducer(broker, {
        ledger: await idempotentLedger(),
        retrier: retrier({ retries: 10, initialRetryTime: 1000, maxRetryTime: 1000, factor: 0, multiplier: 1 }),
        deliveryTimeoutMs: 500,
      });

      const sendPromise = producer.send({ topic, messages: [{ value: 'a' }] });
      // eslint-disable-next-line vitest/valid-expect -- attach the matcher before the fake timer fires
      const assertion = expect(sendPromise).rejects.toBeInstanceOf(KafkaDeliveryTimeoutError);
      await vi.advanceTimersByTimeAsync(500);
      await assertion;

      // The caller was told the send failed, so it must not keep retrying in the background.
      await vi.advanceTimersByTimeAsync(20_000);
      expect(broker.produce).toHaveBeenCalledTimes(1);

      // A gate still held would keep the next send on the partition waiting until its own deadline.
      broker.produce.mockImplementation(() => Promise.resolve(fakeProduceResponse(topic, 0)));
      const nextSend = producer.send({ topic, messages: [{ value: 'b' }] });
      await vi.advanceTimersByTimeAsync(10);
      await expect(nextSend).resolves.toHaveLength(1);
    });

    describe('without linger', () => {
      it('rejects with KafkaDeliveryTimeoutError once the deadline elapses', async () => {
        vi.useFakeTimers();
        const broker = fakeBroker(1);
        broker.produce.mockImplementation(() => new Promise(() => {}));
        const producer = createTestProducer(broker, { lingerMs: 0, deliveryTimeoutMs: 500 });

        const sendPromise = producer.send({ topic, messages: [{ value: 'a' }] });
        let settled = false;
        sendPromise.then(
          () => (settled = true),
          () => (settled = true),
        );

        await vi.advanceTimersByTimeAsync(499);
        expect(settled).toBe(false);
        await vi.advanceTimersByTimeAsync(1);
        await expect(sendPromise).rejects.toBeInstanceOf(KafkaDeliveryTimeoutError);
      });

      it("keeps one deadline timer across sends, unref'd while no send is pending", async () => {
        vi.useFakeTimers();
        const setTimeoutSpy = vi.spyOn(globalThis, 'setTimeout');
        const broker = fakeBroker(1);
        let respond: (value: unknown) => void = () => {};
        broker.produce.mockImplementation(() => new Promise((resolve) => (respond = resolve)));
        const producer = createTestProducer(broker, { lingerMs: 0, deliveryTimeoutMs: 500 });

        for (const value of ['a', 'b']) {
          const sendPromise = producer.send({ topic, messages: [{ value }] });
          await vi.advanceTimersByTimeAsync(0);
          const deadlineTimer = setTimeoutSpy.mock.results[0]?.value as NodeJS.Timeout;
          expect(deadlineTimer.hasRef()).toBe(true);

          respond(fakeProduceResponse(topic, 0));
          await expect(sendPromise).resolves.toHaveLength(1);
          expect(deadlineTimer.hasRef()).toBe(false);
        }

        expect(broker.produce).toHaveBeenCalledTimes(2);
        expect(setTimeoutSpy).toHaveBeenCalledOnce();
      });

      it('creates no AbortController for a send that never waits', async () => {
        const abortController = vi.spyOn(globalThis, 'AbortController');
        const broker = fakeBroker(1);
        const producer = createTestProducer(broker, { lingerMs: 0, deliveryTimeoutMs: 500 });

        await expect(producer.send({ topic, messages: [{ value: 'a' }] })).resolves.toHaveLength(1);
        expect(abortController).not.toHaveBeenCalled();
      });

      it('stops retrying and releases the partition gates once the deadline elapses', async () => {
        vi.useFakeTimers();
        const broker = fakeBroker(1);
        broker.produce.mockImplementation(() => Promise.reject(new KafkaConnectionError('connection reset')));
        const producer = createTestProducer(broker, {
          ledger: await idempotentLedger(),
          lingerMs: 0,
          retrier: retrier({ retries: 10, initialRetryTime: 1000, maxRetryTime: 1000, factor: 0, multiplier: 1 }),
          deliveryTimeoutMs: 500,
        });

        const sendPromise = producer.send({ topic, messages: [{ value: 'a' }] });
        // eslint-disable-next-line vitest/valid-expect -- attach the matcher before the fake timer fires
        const assertion = expect(sendPromise).rejects.toBeInstanceOf(KafkaDeliveryTimeoutError);
        await vi.advanceTimersByTimeAsync(500);
        await assertion;

        await vi.advanceTimersByTimeAsync(20_000);
        expect(broker.produce).toHaveBeenCalledTimes(1);
        expect(vi.getTimerCount()).toBe(0);

        // A gate still held would keep the next send on the partition waiting until its own deadline.
        broker.produce.mockImplementation(() => Promise.resolve(fakeProduceResponse(topic, 0)));
        const nextSend = producer.send({ topic, messages: [{ value: 'b' }] });
        await vi.advanceTimersByTimeAsync(10);
        await expect(nextSend).resolves.toHaveLength(1);
      });

      it('arms no deadline timer when deliveryTimeoutMs is not positive', async () => {
        vi.useFakeTimers();
        const broker = fakeBroker(1);
        const producer = createTestProducer(broker, { lingerMs: 0, deliveryTimeoutMs: 0 });

        const sendPromise = producer.send({ topic, messages: [{ value: 'a' }] });
        expect(vi.getTimerCount()).toBe(0);
        await expect(sendPromise).resolves.toHaveLength(1);
      });
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

  describe('maxRequestSize', () => {
    it('rejects a single record over maxRequestSize immediately, without buffering or dispatching it', async () => {
      const broker = fakeBroker(1);
      const producer = createTestProducer(broker, { lingerMs: 50, maxRequestSize: 10 });

      await expect(producer.send({ topic, messages: [{ value: 'a'.repeat(11) }] })).rejects.toBeInstanceOf(
        KafkaMessageTooLargeError,
      );
      expect(broker.produce).not.toHaveBeenCalled();
    });

    it('rejects a whole call whose combined records exceed maxRequestSize even when no record alone does', async () => {
      const broker = fakeBroker(1);
      const producer = createTestProducer(broker, { maxRequestSize: 10 });

      await expect(
        producer.send({ topic, messages: [{ value: 'abcde' }, { value: 'fghij' }, { value: 'k' }] }),
      ).rejects.toBeInstanceOf(KafkaMessageTooLargeError);
      expect(broker.produce).not.toHaveBeenCalled();
    });

    it('defaults to 1 MiB (1_048_576 bytes)', async () => {
      const broker = fakeBroker(1);
      const producer = createTestProducer(broker);

      await expect(producer.send({ topic, messages: [{ value: 'a'.repeat(1_048_576) }] })).resolves.toBeTruthy();
      await expect(producer.send({ topic, messages: [{ value: 'a'.repeat(1_048_577) }] })).rejects.toBeInstanceOf(
        KafkaMessageTooLargeError,
      );
    });

    it('is configurable', async () => {
      const broker = fakeBroker(1);
      const producer = createTestProducer(broker, { maxRequestSize: 5 });

      await expect(producer.send({ topic, messages: [{ value: 'abcde' }] })).resolves.toBeTruthy();
      await expect(producer.send({ topic, messages: [{ value: 'abcdef' }] })).rejects.toBeInstanceOf(
        KafkaMessageTooLargeError,
      );
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
