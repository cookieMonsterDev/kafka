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
import type { EachBatchHandler, KafkaMessage } from './types';

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
    expect(consumerGroup.commitOffsets).toHaveBeenCalled();
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
        await runner!.stop();
      },
      concurrency: 1,
      heartbeatInterval: 3000,
    });
    runner.scheduleFetchManager = vi.fn();
    await runner.start();

    const batch = new Batch(topicName, 0n, { partition, highWatermark: 5n, messages: [kafkaMessage(4n)] });
    await expect(runner.handleBatch(batch)).resolves.toBeUndefined();
    expect(runner.running).toBe(false);
    expect(consumerGroup.leave).toHaveBeenCalled();
  });
});
