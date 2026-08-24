import { afterEach, describe, expect, it, vi } from 'vitest';
import { KafkaNotImplemented, KafkaNumberOfRetriesExceeded, KafkaProtocolError } from '../errors';
import { InstrumentationEventEmitter } from '../instrumentation/emitter';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createErrorFromCode } from '../protocol/error-codes';
import { TIMESTAMP_TYPES } from '../protocol/enums/timestamp-types';
import { waitFor } from '../utils/wait';
import { Batch } from './batch';
import type { ConsumerGroupHandle } from './consumer-group';
import { Runner } from './runner';
import type { EachBatchHandler, KafkaMessage, OnConsumeEvent } from './types';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });
const REBALANCE_IN_PROGRESS = 27;
const UNKNOWN = -1;

const defaultBatchContext = {
  firstOffset: 0n,
  firstTimestamp: 0n,
  partitionLeaderEpoch: 0,
  inTransaction: false,
  isControlBatch: false,
  lastOffsetDelta: 0,
  producerId: -1n,
  producerEpoch: 0,
  firstSequence: 0,
  maxTimestamp: 0n,
  timestampType: TIMESTAMP_TYPES.CREATE_TIME,
  magicByte: 2,
};

function kafkaMessage(offset: bigint): KafkaMessage {
  return {
    magicByte: 2,
    attributes: 0,
    timestamp: 0n,
    offset,
    key: Buffer.from('1'),
    value: Buffer.from('2'),
    headers: {},
    isControlRecord: false,
    batchContext: defaultBatchContext,
    byteSize: 0,
  };
}

function rebalancingError(): KafkaProtocolError {
  return new KafkaProtocolError(createErrorFromCode(REBALANCE_IN_PROGRESS));
}

describe('consumer/runner', () => {
  let runner: Runner | undefined;
  const topicName = 'topic-test';
  const partition = 0;

  function fakeConsumerGroup(overrides: Partial<Record<string, unknown>> = {}): ConsumerGroupHandle {
    return {
      groupId: 'group',
      memberId: 'member',
      getNodeIds: vi.fn(() => ['1', '2', '3']),
      connect: vi.fn(async () => undefined),
      joinAndSync: vi.fn(async () => undefined),
      leave: vi.fn(async () => undefined),
      fetch: vi.fn(async () => [new Batch(topicName, 0n, { partition, highWatermark: 5n, messages: [] })]),
      resolveOffset: vi.fn(),
      commitOffsets: vi.fn(async () => undefined),
      commitOffsetsIfNecessary: vi.fn(async () => undefined),
      uncommittedOffsets: vi.fn(() => ({ topics: [] })),
      heartbeat: vi.fn(async () => undefined),
      heartbeatDue: vi.fn(() => false),
      assigned: vi.fn(() => []),
      isLeader: vi.fn(() => true),
      isPaused: vi.fn().mockReturnValue(false),
      pause: vi.fn(),
      resume: vi.fn(),
      hasSeekOffset: vi.fn().mockReturnValue(false),
      ...overrides,
    } as unknown as ConsumerGroupHandle;
  }

  afterEach(async () => {
    if (runner) await runner.stop();
  });

  it('recovers from rebalance during start and still schedules the fetch manager', async () => {
    const consumerGroup = fakeConsumerGroup();
    const onCrash = vi.fn();
    runner = new Runner({
      consumerGroup,
      onCrash,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: silentLogger,
      eachBatch: vi.fn(async () => undefined),
      concurrency: 1,
      heartbeatInterval: 3000,
    });
    runner.scheduleFetchManager = vi.fn();
    await runner.start();
    expect(runner.scheduleFetchManager).toHaveBeenCalled();
    expect(onCrash).not.toHaveBeenCalled();
  });

  it('commits offsets during handleBatch', async () => {
    const consumerGroup = fakeConsumerGroup();
    const eachBatch = vi.fn(async () => undefined);
    const onCrash = vi.fn();
    runner = new Runner({
      consumerGroup,
      onCrash,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: silentLogger,
      eachBatch,
      concurrency: 1,
      heartbeatInterval: 3000,
    });
    runner.scheduleFetchManager = vi.fn();
    await runner.start();

    const batch = new Batch(topicName, 0n, {
      partition,
      highWatermark: 5n,
      messages: [kafkaMessage(4n)],
    });
    await runner.handleBatch(batch);
    expect(eachBatch).toHaveBeenCalled();
    expect(consumerGroup.commitOffsetsIfNecessary).toHaveBeenCalled();
    expect(consumerGroup.commitOffsets).not.toHaveBeenCalled();
    expect(onCrash).not.toHaveBeenCalled();
  });

  it('allows providing offsets to commitOffsetsIfNecessary from eachBatch', async () => {
    const consumerGroup = fakeConsumerGroup();
    const eachBatch = vi.fn<EachBatchHandler>(async () => undefined);
    runner = new Runner({
      consumerGroup,
      onCrash: vi.fn(),
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: silentLogger,
      eachBatch,
      concurrency: 1,
      heartbeatInterval: 3000,
    });
    runner.scheduleFetchManager = vi.fn();
    await runner.start();

    const batch = new Batch(topicName, 0n, { partition, highWatermark: 5n, messages: [kafkaMessage(4n)] });
    await runner.handleBatch(batch);

    const payload = eachBatch.mock.calls[0]?.[0];
    expect(payload).toBeDefined();
    const { commitOffsetsIfNecessary } = payload!;
    vi.mocked(consumerGroup.commitOffsetsIfNecessary).mockClear();
    vi.mocked(consumerGroup.commitOffsets).mockClear();

    await commitOffsetsIfNecessary();
    expect(consumerGroup.commitOffsetsIfNecessary).toHaveBeenCalledTimes(1);
    expect(consumerGroup.commitOffsets).toHaveBeenCalledTimes(0);

    vi.mocked(consumerGroup.commitOffsetsIfNecessary).mockClear();
    vi.mocked(consumerGroup.commitOffsets).mockClear();

    const offsets = { topics: [{ topic: topicName, partitions: [{ offset: 1n, partition: 0 }] }] };
    await commitOffsetsIfNecessary(offsets);
    expect(consumerGroup.commitOffsetsIfNecessary).toHaveBeenCalledTimes(0);
    expect(consumerGroup.commitOffsets).toHaveBeenCalledTimes(1);
    expect(consumerGroup.commitOffsets).toHaveBeenCalledWith(offsets);
  });

  it('does not call resolveOffset with the last offset when eachBatchAutoResolve is false', async () => {
    const consumerGroup = fakeConsumerGroup();
    runner = new Runner({
      consumerGroup,
      onCrash: vi.fn(),
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: silentLogger,
      eachBatch: vi.fn(async () => undefined),
      eachBatchAutoResolve: false,
      concurrency: 1,
      heartbeatInterval: 3000,
    });
    runner.scheduleFetchManager = vi.fn();
    await runner.start();

    const batch = new Batch(topicName, 0n, { partition, highWatermark: 5n, messages: [kafkaMessage(4n)] });
    await runner.handleBatch(batch);
    expect(consumerGroup.resolveOffset).not.toHaveBeenCalled();
  });

  it('does not commit offsets when autoCommit is false', async () => {
    const consumerGroup = fakeConsumerGroup();
    const eachBatch: EachBatchHandler = async ({ uncommittedOffsets }) => {
      uncommittedOffsets();
    };
    runner = new Runner({
      consumerGroup,
      onCrash: vi.fn(),
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: silentLogger,
      eachBatch,
      autoCommit: false,
      concurrency: 1,
      heartbeatInterval: 3000,
    });
    runner.scheduleFetchManager = vi.fn();
    await runner.start();

    const batch = new Batch(topicName, 0n, { partition, highWatermark: 5n, messages: [kafkaMessage(4n)] });
    await runner.handleBatch(batch);

    expect(consumerGroup.commitOffsets).not.toHaveBeenCalled();
    expect(consumerGroup.commitOffsetsIfNecessary).not.toHaveBeenCalled();
    expect(consumerGroup.uncommittedOffsets).toHaveBeenCalled();
  });

  it('calls onCrash for unknown join errors', async () => {
    const unknownError = new KafkaProtocolError(createErrorFromCode(UNKNOWN));
    const onCrash = vi.fn();
    const consumerGroup = fakeConsumerGroup({
      joinAndSync: vi.fn(async () => {
        throw unknownError;
      }),
    });
    runner = new Runner({
      consumerGroup,
      onCrash,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: silentLogger,
      eachBatch: vi.fn(async () => undefined),
      concurrency: 1,
      heartbeatInterval: 3000,
    });
    runner.scheduleFetchManager = vi.fn();
    await runner.start();
    expect(runner.scheduleFetchManager).not.toHaveBeenCalled();
    expect(onCrash).toHaveBeenCalledWith(unknownError);
  });

  it('crashes on KafkaNotImplemented errors from fetch', async () => {
    const notImplementedError = new KafkaNotImplemented('not implemented');
    const onCrash = vi.fn();
    const consumerGroup = fakeConsumerGroup({
      fetch: vi.fn(async () => {
        throw notImplementedError;
      }),
    });
    runner = new Runner({
      consumerGroup,
      onCrash,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: silentLogger,
      eachBatch: vi.fn(async () => undefined),
      concurrency: 1,
      heartbeatInterval: 3000,
    });
    await runner.start();
    await waitFor(() => onCrash.mock.calls.length > 0);
    expect(onCrash).toHaveBeenCalledWith(notImplementedError);
  });

  it('commits offsets while running', async () => {
    const consumerGroup = fakeConsumerGroup();
    runner = new Runner({
      consumerGroup,
      onCrash: vi.fn(),
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: silentLogger,
      eachBatch: vi.fn(async () => undefined),
      concurrency: 1,
      heartbeatInterval: 3000,
    });
    await runner.start();
    const offsets = { topics: [{ topic: topicName, partitions: [{ offset: 1n, partition }] }] };
    await runner.commitOffsets(offsets);
    expect(consumerGroup.commitOffsets).toHaveBeenCalledWith(offsets);
  });

  it('throws when commitOffsets hits a rebalance error', async () => {
    const error = rebalancingError();
    const consumerGroup = fakeConsumerGroup({
      commitOffsets: vi.fn(async () => {
        throw error;
      }),
    });
    runner = new Runner({
      consumerGroup,
      onCrash: vi.fn(),
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: silentLogger,
      eachBatch: vi.fn(async () => undefined),
      concurrency: 1,
      heartbeatInterval: 3000,
      retry: { retries: 0 },
    });
    runner.scheduleFetchManager = vi.fn();
    await runner.start();
    const offsets = { topics: [{ topic: topicName, partitions: [{ offset: 1n, partition }] }] };
    await expect(runner.commitOffsets(offsets)).rejects.toThrow(error.message);
  });

  it('catches exceptions in parallel eachBatch processing', async () => {
    const onCrash = vi.fn();
    const consumerGroup = fakeConsumerGroup();
    const batch = new Batch(topicName, 0n, { partition, highWatermark: 5n, messages: [kafkaMessage(4n)] });
    vi.mocked(consumerGroup.fetch)
      .mockImplementationOnce(async () => new Promise((resolve) => setTimeout(() => resolve([]), 100)))
      .mockImplementationOnce(async () => [batch]);

    runner = new Runner({
      consumerGroup,
      onCrash,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: silentLogger,
      eachBatchAutoResolve: false,
      eachBatch: async () => {
        throw new Error('Error while processing batches in parallel');
      },
      concurrency: 10,
      retry: { retries: 0 },
      heartbeatInterval: 3000,
    });
    await runner.start();
    await waitFor(() => onCrash.mock.calls.length > 0);
    expect(onCrash).toHaveBeenCalledWith(expect.any(KafkaNumberOfRetriesExceeded));
  });

  it('stops while joinAndSync is still in progress', async () => {
    let releaseJoin: () => void = () => {};
    const joinGate = new Promise<void>((resolve) => {
      releaseJoin = resolve;
    });
    const consumerGroup = fakeConsumerGroup({
      joinAndSync: vi.fn(async () => joinGate),
    });
    const onCrash = vi.fn();
    runner = new Runner({
      consumerGroup,
      onCrash,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: silentLogger,
      eachBatch: vi.fn(async () => undefined),
      concurrency: 1,
      heartbeatInterval: 3000,
    });
    runner.scheduleFetchManager = vi.fn();

    const startPromise = runner.start();
    await runner.stop();
    expect(runner.shuttingDown).toBe(true);
    expect(runner.running).toBe(false);

    releaseJoin();
    await startPromise;

    expect(runner.scheduleFetchManager).not.toHaveBeenCalled();
    expect(consumerGroup.leave).toHaveBeenCalled();
    expect(onCrash).not.toHaveBeenCalled();
  });

  it('does not deadlock when stop is called from eachBatch', async () => {
    const consumerGroup = fakeConsumerGroup();
    const onCrash = vi.fn();
    runner = new Runner({
      consumerGroup,
      onCrash,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: silentLogger,
      eachBatch: async () => {
        void runner!.stop();
      },
      concurrency: 1,
      heartbeatInterval: 3000,
    });
    runner.scheduleFetchManager = vi.fn();
    await runner.start();

    const batch = new Batch(topicName, 0n, { partition, highWatermark: 5n, messages: [kafkaMessage(4n)] });
    await expect(runner.handleBatch(batch)).resolves.toBeUndefined();
    await runner.stop();
    expect(runner.running).toBe(false);
    expect(consumerGroup.leave).toHaveBeenCalled();
  });

  it('does not heartbeat or OffsetCommit after every eachMessage record', async () => {
    const consumerGroup = fakeConsumerGroup();
    vi.mocked(consumerGroup.heartbeatDue).mockReturnValue(false);
    const offsets: bigint[] = [];
    runner = new Runner({
      consumerGroup,
      onCrash: vi.fn(),
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: silentLogger,
      eachMessage: async ({ message }) => {
        offsets.push(message.offset);
      },
      concurrency: 1,
      heartbeatInterval: 3000,
    });
    runner.scheduleFetchManager = vi.fn();
    await runner.start();

    const batch = new Batch(topicName, 0n, {
      partition,
      highWatermark: 5n,
      messages: [kafkaMessage(1n), kafkaMessage(2n), kafkaMessage(3n)],
    });
    await runner.handleBatch(batch);

    expect(offsets).toEqual([1n, 2n, 3n]);
    expect(consumerGroup.heartbeat).not.toHaveBeenCalled();
    expect(consumerGroup.commitOffsetsIfNecessary).toHaveBeenCalledTimes(1);
    expect(consumerGroup.commitOffsets).not.toHaveBeenCalled();
  });

  it('heartbeats from the runner only when the interval has elapsed', async () => {
    const consumerGroup = fakeConsumerGroup();
    vi.mocked(consumerGroup.heartbeatDue).mockReturnValueOnce(false).mockReturnValueOnce(false).mockReturnValue(true);
    runner = new Runner({
      consumerGroup,
      onCrash: vi.fn(),
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: silentLogger,
      eachMessage: async () => undefined,
      concurrency: 1,
      heartbeatInterval: 3000,
    });
    runner.scheduleFetchManager = vi.fn();
    await runner.start();

    const batch = new Batch(topicName, 0n, {
      partition,
      highWatermark: 5n,
      messages: [kafkaMessage(1n), kafkaMessage(2n), kafkaMessage(3n)],
    });
    await runner.handleBatch(batch);

    expect(consumerGroup.heartbeatDue).toHaveBeenCalled();
    expect(consumerGroup.heartbeat).toHaveBeenCalledTimes(2);
    expect(consumerGroup.commitOffsetsIfNecessary).toHaveBeenCalledTimes(1);
    expect(consumerGroup.commitOffsets).not.toHaveBeenCalled();
  });

  it('honors autoCommitInterval after eachBatch instead of committing every batch', async () => {
    const consumerGroup = fakeConsumerGroup();
    runner = new Runner({
      consumerGroup,
      onCrash: vi.fn(),
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: silentLogger,
      eachBatch: vi.fn(async () => undefined),
      concurrency: 1,
      heartbeatInterval: 3000,
    });
    runner.scheduleFetchManager = vi.fn();
    await runner.start();

    const batch = new Batch(topicName, 0n, { partition, highWatermark: 5n, messages: [kafkaMessage(4n)] });
    await runner.handleBatch(batch);
    await runner.handleBatch(batch);

    expect(consumerGroup.commitOffsetsIfNecessary).toHaveBeenCalledTimes(2);
    expect(consumerGroup.commitOffsets).not.toHaveBeenCalled();
  });

  it('commits offsets on stop so disconnect does not drop the last batch', async () => {
    const consumerGroup = fakeConsumerGroup();
    runner = new Runner({
      consumerGroup,
      onCrash: vi.fn(),
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: silentLogger,
      eachMessage: async () => undefined,
      concurrency: 1,
      heartbeatInterval: 3000,
    });
    runner.scheduleFetchManager = vi.fn();
    await runner.start();
    vi.mocked(consumerGroup.commitOffsets).mockClear();
    await runner.stop();
    expect(consumerGroup.commitOffsets).toHaveBeenCalledTimes(1);
    runner = undefined;
  });

  describe('hooks', () => {
    it('fires onConsume in registration order before eachMessage, once per message', async () => {
      const consumerGroup = fakeConsumerGroup();
      const order: string[] = [];
      const first = vi.fn((_event: OnConsumeEvent) => {
        order.push('onConsume-1');
      });
      const second = vi.fn((_event: OnConsumeEvent) => {
        order.push('onConsume-2');
      });
      const eachMessage = vi.fn(async () => {
        order.push('eachMessage');
      });
      runner = new Runner({
        consumerGroup,
        onCrash: vi.fn(),
        instrumentationEmitter: new InstrumentationEventEmitter(),
        logger: silentLogger,
        eachMessage,
        concurrency: 1,
        heartbeatInterval: 3000,
        hooks: { onConsume: [first, second] },
      });
      runner.scheduleFetchManager = vi.fn();
      await runner.start();

      const batch = new Batch(topicName, 0n, {
        partition,
        highWatermark: 5n,
        messages: [kafkaMessage(1n), kafkaMessage(2n)],
      });
      await runner.handleBatch(batch);

      expect(order).toEqual(['onConsume-1', 'onConsume-2', 'eachMessage', 'onConsume-1', 'onConsume-2', 'eachMessage']);
      expect(first).toHaveBeenCalledWith(
        expect.objectContaining({ topic: topicName, partition, message: expect.any(Object) }),
      );
      const event = first.mock.calls[0]?.[0];
      expect(event?.batch).toBeUndefined();
    });

    it('fires onConsume once per batch, before eachBatch', async () => {
      const consumerGroup = fakeConsumerGroup();
      const order: string[] = [];
      const onConsume = vi.fn((_event: OnConsumeEvent) => {
        order.push('onConsume');
      });
      const eachBatch = vi.fn(async () => {
        order.push('eachBatch');
      });
      runner = new Runner({
        consumerGroup,
        onCrash: vi.fn(),
        instrumentationEmitter: new InstrumentationEventEmitter(),
        logger: silentLogger,
        eachBatch,
        concurrency: 1,
        heartbeatInterval: 3000,
        hooks: { onConsume: [onConsume] },
      });
      runner.scheduleFetchManager = vi.fn();
      await runner.start();

      const batch = new Batch(topicName, 0n, {
        partition,
        highWatermark: 5n,
        messages: [kafkaMessage(1n), kafkaMessage(2n)],
      });
      await runner.handleBatch(batch);

      expect(order).toEqual(['onConsume', 'eachBatch']);
      expect(onConsume).toHaveBeenCalledTimes(1);
      const event = onConsume.mock.calls[0]?.[0];
      expect(event?.message).toBeUndefined();
      expect(event?.batch).toBe(batch);
    });

    it('does not fail eachMessage processing when an onConsume hook throws', async () => {
      const consumerGroup = fakeConsumerGroup();
      const throwingHook = vi.fn(() => {
        throw new Error('onConsume boom');
      });
      const eachMessage = vi.fn(async () => undefined);
      runner = new Runner({
        consumerGroup,
        onCrash: vi.fn(),
        instrumentationEmitter: new InstrumentationEventEmitter(),
        logger: silentLogger,
        eachMessage,
        concurrency: 1,
        heartbeatInterval: 3000,
        hooks: { onConsume: [throwingHook] },
      });
      runner.scheduleFetchManager = vi.fn();
      await runner.start();

      const batch = new Batch(topicName, 0n, { partition, highWatermark: 5n, messages: [kafkaMessage(4n)] });
      await expect(runner.handleBatch(batch)).resolves.toBeUndefined();

      expect(throwingHook).toHaveBeenCalledTimes(1);
      expect(eachMessage).toHaveBeenCalledTimes(1);
      expect(consumerGroup.resolveOffset).toHaveBeenCalledWith({ topic: topicName, partition, offset: 4n });
    });
  });

  it('does not commit on stop when autoCommit is false', async () => {
    const consumerGroup = fakeConsumerGroup();
    runner = new Runner({
      consumerGroup,
      onCrash: vi.fn(),
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: silentLogger,
      eachMessage: async () => undefined,
      autoCommit: false,
      concurrency: 1,
      heartbeatInterval: 3000,
    });
    runner.scheduleFetchManager = vi.fn();
    await runner.start();
    await runner.stop();
    expect(consumerGroup.commitOffsets).not.toHaveBeenCalled();
    expect(consumerGroup.commitOffsetsIfNecessary).not.toHaveBeenCalled();
    runner = undefined;
  });

  describe('maxRecords', () => {
    function messagesRange(count: number, startOffset = 1n): KafkaMessage[] {
      return Array.from({ length: count }, (_, index) => kafkaMessage(startOffset + BigInt(index)));
    }

    it('defaults to 500-record slices per poll for eachMessage even when the batch is much larger', async () => {
      const consumerGroup = fakeConsumerGroup();
      const seenOffsets: bigint[] = [];
      runner = new Runner({
        consumerGroup,
        onCrash: vi.fn(),
        instrumentationEmitter: new InstrumentationEventEmitter(),
        logger: silentLogger,
        eachMessage: async ({ message }) => {
          seenOffsets.push(message.offset);
        },
        concurrency: 1,
        heartbeatInterval: 3000,
      });
      runner.scheduleFetchManager = vi.fn();
      await runner.start();

      const messages = messagesRange(1200, 1n);
      const batch = new Batch(topicName, 0n, { partition, highWatermark: 2000n, messages });
      await runner.handleBatch(batch);

      // Every record is still delivered, in order, none dropped or duplicated across slices.
      expect(seenOffsets).toHaveLength(1200);
      expect(seenOffsets[0]).toBe(1n);
      expect(seenOffsets[seenOffsets.length - 1]).toBe(1200n);

      // Two internal 500-record checkpoints (after offsets 500 and 1000) plus the final commit
      // `handleBatch` issues once `processEachMessage` returns.
      expect(consumerGroup.commitOffsetsIfNecessary).toHaveBeenCalledTimes(3);
      expect(consumerGroup.commitOffsets).not.toHaveBeenCalled();
    });

    it('delivers a batch smaller than maxRecords in a single internal cycle', async () => {
      const consumerGroup = fakeConsumerGroup();
      const seenOffsets: bigint[] = [];
      runner = new Runner({
        consumerGroup,
        onCrash: vi.fn(),
        instrumentationEmitter: new InstrumentationEventEmitter(),
        logger: silentLogger,
        eachMessage: async ({ message }) => {
          seenOffsets.push(message.offset);
        },
        maxRecords: 5,
        concurrency: 1,
        heartbeatInterval: 3000,
      });
      runner.scheduleFetchManager = vi.fn();
      await runner.start();

      const batch = new Batch(topicName, 0n, { partition, highWatermark: 10n, messages: messagesRange(3, 1n) });
      await runner.handleBatch(batch);

      expect(seenOffsets).toEqual([1n, 2n, 3n]);
      // 3 < maxRecords(5): no mid-batch checkpoint, only the end-of-batch commit.
      expect(consumerGroup.commitOffsetsIfNecessary).toHaveBeenCalledTimes(1);
    });

    it('does not truncate eachBatch by default even for a very large batch', async () => {
      const consumerGroup = fakeConsumerGroup();
      const eachBatch = vi.fn<EachBatchHandler>(async () => undefined);
      runner = new Runner({
        consumerGroup,
        onCrash: vi.fn(),
        instrumentationEmitter: new InstrumentationEventEmitter(),
        logger: silentLogger,
        eachBatch,
        concurrency: 1,
        heartbeatInterval: 3000,
      });
      runner.scheduleFetchManager = vi.fn();
      await runner.start();

      const messages = messagesRange(10_000, 1n);
      const batch = new Batch(topicName, 0n, { partition, highWatermark: 20_000n, messages });
      await runner.handleBatch(batch);

      expect(eachBatch).toHaveBeenCalledTimes(1);
      expect(eachBatch.mock.calls[0]?.[0].batch.messages).toHaveLength(10_000);
    });

    it('slices eachBatch into maxRecords-sized sub-batches when set explicitly', async () => {
      const consumerGroup = fakeConsumerGroup();
      const receivedSizes: number[] = [];
      const receivedOffsets: bigint[] = [];
      const eachBatch: EachBatchHandler = async ({ batch: slice, resolveOffset }) => {
        receivedSizes.push(slice.messages.length);
        for (const message of slice.messages) {
          receivedOffsets.push(message.offset);
          resolveOffset(message.offset);
        }
      };
      runner = new Runner({
        consumerGroup,
        onCrash: vi.fn(),
        instrumentationEmitter: new InstrumentationEventEmitter(),
        logger: silentLogger,
        eachBatch,
        maxRecords: 5,
        concurrency: 1,
        heartbeatInterval: 3000,
      });
      runner.scheduleFetchManager = vi.fn();
      await runner.start();

      const messages = messagesRange(12, 1n);
      const batch = new Batch(topicName, 0n, { partition, highWatermark: 20n, messages });
      await runner.handleBatch(batch);

      expect(receivedSizes).toEqual([5, 5, 2]);
      expect(receivedOffsets).toEqual(messages.map((message) => message.offset));
      // Checkpointed between the 2 slice boundaries, plus the final commit after the last slice.
      expect(consumerGroup.commitOffsetsIfNecessary).toHaveBeenCalledTimes(3);
    });

    it('does not pass maxRecords into the Fetch call, leaving maxBytes/maxBytesPerPartition untouched', async () => {
      const consumerGroup = fakeConsumerGroup();
      runner = new Runner({
        consumerGroup,
        onCrash: vi.fn(),
        instrumentationEmitter: new InstrumentationEventEmitter(),
        logger: silentLogger,
        eachMessage: async () => undefined,
        maxRecords: 1,
        concurrency: 1,
        heartbeatInterval: 3000,
      });
      runner.scheduleFetchManager = vi.fn();
      await runner.start();

      await runner.fetch('1');

      // `Runner#fetch` only ever forwards the node id: `maxRecords` never reaches the Fetch
      // request, so the wire-level `maxBytes`/`maxBytesPerPartition` configured on the consumer
      // group (owned by `ConsumerGroup`, not `Runner`) are never altered by it.
      expect(consumerGroup.fetch).toHaveBeenCalledWith('1');
      expect(consumerGroup.fetch).toHaveBeenCalledTimes(1);
    });
  });
});
