import { describe, expect, it, vi } from 'vitest';
import { InstrumentationEventEmitter } from '../instrumentation/emitter';
import type { InstrumentationEvent } from '../instrumentation/event';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createErrorFromCode, ERROR_CODES } from '../protocol/error-codes';
import { SHARE_ACKNOWLEDGE_TYPE } from './acknowledge-types';
import { FETCH, FETCH_START } from './instrumentation-events';
import { ShareBatch } from './share-batch';
import type { ShareGroup } from './share-group';
import { ShareRunner } from './share-runner';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

const BATCH_CONTEXT = {
  firstOffset: 10n,
  firstTimestamp: 1n,
  partitionLeaderEpoch: 0,
  inTransaction: false,
  isControlBatch: false,
  lastOffsetDelta: 0,
  producerId: -1n,
  producerEpoch: -1,
  firstSequence: -1,
  maxTimestamp: 1n,
  timestampType: 0,
  magicByte: 2,
} as const;

function shareBatch(partition = 0): ShareBatch {
  return new ShareBatch('events', {
    partition,
    messages: [
      {
        magicByte: 2,
        attributes: 0,
        timestamp: 1n,
        offset: 10n,
        key: Buffer.from('k'),
        value: Buffer.from('v'),
        headers: {},
        isControlRecord: false,
        byteSize: 1,
        batchContext: BATCH_CONTEXT,
      },
    ],
    acquiredRecords: [{ firstOffset: 10n, lastOffset: 10n, deliveryCount: 1 }],
  });
}

/** Resolves empty after a short wait, as a long-polling ShareFetch that acquired nothing would. */
async function emptyFetch(): Promise<ShareBatch[]> {
  await new Promise((resolve) => setTimeout(resolve, 5));
  return [];
}

function errorOfType(type: string): Error {
  const code = ERROR_CODES.find((entry) => entry.type === type)?.code;
  if (code == null) throw new Error(`Unknown error type ${type}`);
  return createErrorFromCode(code);
}

function createShareGroup(overrides: Record<string, unknown> = {}) {
  const fetch = vi.fn().mockResolvedValueOnce([shareBatch()]).mockImplementation(emptyFetch);
  const acknowledge = vi.fn();
  const closeShareSessions = vi.fn().mockResolvedValue(undefined);
  const leave = vi.fn().mockResolvedValue(undefined);
  const shareGroup = {
    groupId: 'share-1',
    memberId: 'member-1',
    heartbeatIntervalMs: 50,
    connect: vi.fn().mockResolvedValue(undefined),
    joinAndSync: vi.fn().mockResolvedValue(undefined),
    leave,
    disconnect: vi.fn().mockResolvedValue(undefined),
    heartbeat: vi.fn().mockResolvedValue(undefined),
    heartbeatDue: vi.fn().mockReturnValue(false),
    getNodeIds: vi.fn().mockReturnValue(['1']),
    hasAssignment: vi.fn().mockReturnValue(true),
    recoverFromFetch: vi.fn(),
    fetch,
    acknowledge,
    closeShareSessions,
    ...overrides,
  } as unknown as ShareGroup;

  return { shareGroup, fetch, acknowledge, closeShareSessions, leave };
}

describe('share-consumer/share-runner', () => {
  it('accepts handled records, then closes the share sessions before leaving on stop', async () => {
    const { shareGroup, acknowledge, closeShareSessions, leave } = createShareGroup();
    const handled: bigint[] = [];
    const runner = new ShareRunner({
      logger: silentLogger,
      shareGroup,
      heartbeatInterval: 50,
      retry: { retries: 0 },
      eachMessage: async ({ message }) => {
        handled.push(message.offset);
        runner.shuttingDown = true;
      },
      onCrash: vi.fn(),
    });

    await runner.start();
    await vi.waitFor(() => expect(handled).toEqual([10n]));
    await runner.stop();

    expect(acknowledge).toHaveBeenCalledWith(expect.objectContaining({ partition: 0 }), SHARE_ACKNOWLEDGE_TYPE.ACCEPT);
    expect(closeShareSessions).toHaveBeenCalledTimes(1);
    expect(closeShareSessions.mock.invocationCallOrder[0]).toBeLessThan(leave.mock.invocationCallOrder[0] ?? 0);
  });

  it('releases acquired records and crashes when the handler fails', async () => {
    const { shareGroup, acknowledge } = createShareGroup();
    const onCrash = vi.fn();
    const runner = new ShareRunner({
      logger: silentLogger,
      shareGroup,
      heartbeatInterval: 50,
      retry: { retries: 0 },
      eachMessage: async () => {
        throw new Error('boom');
      },
      onCrash,
    });

    await runner.start();
    await vi.waitFor(() => expect(onCrash).toHaveBeenCalled());
    runner.shuttingDown = true;
    await runner.stop();

    expect(acknowledge).toHaveBeenCalledTimes(1);
    expect(acknowledge).toHaveBeenCalledWith(expect.any(ShareBatch), SHARE_ACKNOWLEDGE_TYPE.RELEASE);
  });

  it('acknowledges an eachBatch batch with the type its handler chose', async () => {
    const { shareGroup, acknowledge } = createShareGroup();
    const seen: string[] = [];
    const runner = new ShareRunner({
      logger: silentLogger,
      shareGroup,
      heartbeatInterval: 50,
      retry: { retries: 0 },
      eachBatch: async ({ batch, acknowledge: acknowledgeBatch }) => {
        seen.push(`${batch.topic}:${batch.partition}`);
        acknowledgeBatch(SHARE_ACKNOWLEDGE_TYPE.REJECT);
        runner.shuttingDown = true;
      },
      onCrash: vi.fn(),
    });

    await runner.start();
    await vi.waitFor(() => expect(seen).toEqual(['events:0']));
    await runner.stop();

    expect(acknowledge).toHaveBeenCalledTimes(1);
    expect(acknowledge).toHaveBeenCalledWith(expect.any(ShareBatch), SHARE_ACKNOWLEDGE_TYPE.REJECT);
  });

  it.each([
    { eachBatchAutoAck: true, expected: [[expect.any(ShareBatch), SHARE_ACKNOWLEDGE_TYPE.ACCEPT]] },
    { eachBatchAutoAck: false, expected: [] },
  ])(
    'with eachBatchAutoAck $eachBatchAutoAck, settles a batch its handler did not acknowledge',
    async ({ eachBatchAutoAck, expected }) => {
      const { shareGroup, acknowledge } = createShareGroup();
      let handled = false;
      const runner = new ShareRunner({
        logger: silentLogger,
        shareGroup,
        heartbeatInterval: 50,
        retry: { retries: 0 },
        eachBatchAutoAck,
        eachBatch: async () => {
          handled = true;
          runner.shuttingDown = true;
        },
        onCrash: vi.fn(),
      });

      await runner.start();
      await vi.waitFor(() => expect(handled).toBe(true));
      await runner.stop();

      expect(acknowledge.mock.calls).toEqual(expected);
    },
  );

  it('fetches from each assigned node in parallel', async () => {
    const fetch = vi.fn(async () => {
      await new Promise((resolve) => setTimeout(resolve, 40));
      return [];
    });
    const { shareGroup } = createShareGroup({ fetch, getNodeIds: vi.fn().mockReturnValue(['1', '2']) });
    const runner = new ShareRunner({
      logger: silentLogger,
      shareGroup,
      heartbeatInterval: 50,
      retry: { retries: 0 },
      eachMessage: async () => undefined,
      onCrash: vi.fn(),
    });

    const started = Date.now();
    await runner.start();
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledWith('1'));
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledWith('2'));
    const elapsed = Date.now() - started;
    runner.shuttingDown = true;
    await runner.stop();
    expect(elapsed).toBeLessThan(80);
  });

  it('drops fetched batches whose partition is no longer assigned', async () => {
    const { shareGroup } = createShareGroup({ hasAssignment: vi.fn().mockReturnValue(false) });
    const instrumentationEmitter = new InstrumentationEventEmitter();
    const fetches: { numberOfBatches: number }[] = [];
    instrumentationEmitter.addListener(FETCH, (event: InstrumentationEvent<unknown>) => {
      fetches.push(event.payload as { numberOfBatches: number });
    });
    const eachMessage = vi.fn();
    const runner = new ShareRunner({
      logger: silentLogger,
      shareGroup,
      heartbeatInterval: 50,
      retry: { retries: 0 },
      instrumentationEmitter,
      eachMessage,
      onCrash: vi.fn(),
    });

    await runner.start();
    await vi.waitFor(() => expect(fetches.length).toBeGreaterThan(0));
    runner.shuttingDown = true;
    await runner.stop();

    expect(fetches[0]?.numberOfBatches).toBe(0);
    expect(eachMessage).not.toHaveBeenCalled();
  });

  it('crashes on a non-retriable fetch error instead of retrying it forever', async () => {
    const fetch = vi.fn().mockRejectedValue(errorOfType('TOPIC_AUTHORIZATION_FAILED'));
    const { shareGroup } = createShareGroup({ fetch });
    const onCrash = vi.fn();
    const runner = new ShareRunner({
      logger: silentLogger,
      shareGroup,
      heartbeatInterval: 1000,
      retry: { retries: 0 },
      onCrash,
    });

    try {
      await runner.start();
      await vi.waitFor(() => expect(onCrash).toHaveBeenCalledWith(expect.objectContaining({ retriable: false })));
      expect(fetch).toHaveBeenCalledTimes(1);
    } finally {
      runner.shuttingDown = true;
      await runner.stop();
    }
  });

  it('backs off before retrying a retriable fetch error', async () => {
    const fetch = vi.fn().mockRejectedValue(errorOfType('LEADER_NOT_AVAILABLE'));
    const { shareGroup } = createShareGroup({ fetch });
    const onCrash = vi.fn();
    const runner = new ShareRunner({
      logger: silentLogger,
      shareGroup,
      heartbeatInterval: 1000,
      retry: { retries: 0 },
      onCrash,
    });

    try {
      await runner.start();
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(onCrash).not.toHaveBeenCalled();
      expect(fetch.mock.calls.length).toBeLessThan(5);
    } finally {
      runner.shuttingDown = true;
      await runner.stop();
    }
  });

  it('emits FETCH_START and FETCH around each fetch round', async () => {
    const { shareGroup } = createShareGroup();
    const instrumentationEmitter = new InstrumentationEventEmitter();
    const fetchStarts: unknown[] = [];
    const fetches: unknown[] = [];
    instrumentationEmitter.addListener(FETCH_START, (event: InstrumentationEvent<unknown>) => {
      fetchStarts.push(event.payload);
    });
    instrumentationEmitter.addListener(FETCH, (event: InstrumentationEvent<unknown>) => {
      fetches.push(event.payload);
    });

    const runner = new ShareRunner({
      logger: silentLogger,
      shareGroup,
      heartbeatInterval: 50,
      retry: { retries: 0 },
      instrumentationEmitter,
      eachMessage: async () => {
        runner.shuttingDown = true;
      },
      onCrash: vi.fn(),
    });

    await runner.start();
    await vi.waitFor(() => expect(fetches.length).toBeGreaterThan(0));
    await runner.stop();

    expect(fetchStarts[0]).toEqual({ nodeId: '1' });
    expect(fetches[0]).toEqual({ nodeId: '1', numberOfBatches: 1, duration: expect.any(Number) });
  });

  it('observes a heartbeat-loop rejection instead of leaving it unhandled, and invokes onCrash', async () => {
    const heartbeatError = new Error('heartbeat boom');
    const { shareGroup } = createShareGroup({
      heartbeat: vi.fn().mockRejectedValue(heartbeatError),
      getNodeIds: vi.fn().mockReturnValue([]),
    });
    const onCrash = vi.fn().mockImplementation((error: Error) => {
      throw error;
    });

    const unhandledRejections: unknown[] = [];
    const onUnhandledRejection = (reason: unknown): void => {
      unhandledRejections.push(reason);
    };
    process.on('unhandledRejection', onUnhandledRejection);

    const runner = new ShareRunner({
      logger: silentLogger,
      shareGroup,
      heartbeatInterval: 10,
      retry: { retries: 0 },
      onCrash,
    });

    try {
      await runner.start();
      await vi.waitFor(() => expect(onCrash).toHaveBeenCalledWith(heartbeatError));
      await vi.waitFor(() => expect(runner.running).toBe(false));
      await new Promise((resolve) => setImmediate(resolve));
      expect(unhandledRejections).toEqual([]);
    } finally {
      process.off('unhandledRejection', onUnhandledRejection);
      runner.shuttingDown = true;
      await runner.stop();
    }
  });

  it('observes a fetch-loop rejection instead of leaving it unhandled, and invokes onCrash', async () => {
    const fetchError = new Error('fetch boom');
    const { shareGroup } = createShareGroup({ fetch: vi.fn().mockRejectedValue(fetchError) });
    const onCrash = vi.fn().mockImplementation((error: Error) => {
      throw error;
    });

    const unhandledRejections: unknown[] = [];
    const onUnhandledRejection = (reason: unknown): void => {
      unhandledRejections.push(reason);
    };
    process.on('unhandledRejection', onUnhandledRejection);

    const runner = new ShareRunner({
      logger: silentLogger,
      shareGroup,
      heartbeatInterval: 1000,
      retry: { retries: 0 },
      onCrash,
    });

    try {
      await runner.start();
      await vi.waitFor(() => expect(onCrash).toHaveBeenCalledWith(fetchError));
      await vi.waitFor(() => expect(runner.running).toBe(false));
      await new Promise((resolve) => setImmediate(resolve));
      expect(unhandledRejections).toEqual([]);
    } finally {
      process.off('unhandledRejection', onUnhandledRejection);
      runner.shuttingDown = true;
      await runner.stop();
    }
  });
});
