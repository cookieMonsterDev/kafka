import { describe, expect, it, vi, type Mock } from 'vitest';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createFetcher, trackFetchBatch } from './fetcher';
import type { WorkerQueue } from './worker-queue';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

type Batch = { topic: string; partition: number };

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

interface PushOptions {
  onEnqueued?: (batch: Batch) => void;
  onSettled?: (batch: Batch) => void;
}

function mockQueue(onPushed?: (batch: Batch, options?: PushOptions) => void) {
  const pushForNode = vi.fn(async (_nodeId: string, batches: Batch[], options?: PushOptions) => {
    for (const batch of batches) {
      options?.onEnqueued?.(batch);
      onPushed?.(batch, options);
    }
  });
  return {
    pushForNode,
    interrupt: vi.fn(),
  } as unknown as WorkerQueue<Batch> & { pushForNode: ReturnType<typeof vi.fn>; interrupt: ReturnType<typeof vi.fn> };
}

describe('consumer/fetcher', () => {
  it('returns immediately from stop when not running', async () => {
    const fetcher = createFetcher({
      nodeId: '1',
      workerQueue: mockQueue(),
      partitionAssignments: new Map(),
      fetch: vi.fn(async () => []),
      logger: silentLogger,
    });
    await expect(fetcher.stop()).resolves.toBeUndefined();
  });

  it('ignores a second start while a fetch is in flight', async () => {
    const gate = deferred<Batch[]>();
    const fetch = vi.fn(() => gate.promise);
    const workerQueue = mockQueue();
    const fetcher = createFetcher({
      nodeId: '1',
      workerQueue,
      partitionAssignments: new Map(),
      fetch,
      logger: silentLogger,
    });

    const started = fetcher.start();
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledTimes(1));
    await fetcher.start();
    expect(fetch).toHaveBeenCalledTimes(1);

    const stopping = fetcher.stop();
    gate.resolve([]);
    await started;
    await stopping;
  });

  function fetchResponses(responses: Batch[][]): {
    fetch: Mock<(nodeId: string) => Promise<Batch[]>>;
    release: () => void;
  } {
    const gate = deferred<Batch[]>();
    const pending = [...responses];
    const fetch = vi.fn(async (_nodeId: string) => pending.shift() ?? gate.promise);
    return { fetch, release: () => gate.resolve([]) };
  }

  function batchSettler(): {
    workerQueue: ReturnType<typeof mockQueue>;
    settle: (batch: Batch) => void;
  } {
    const settledByBatch = new Map<Batch, () => void>();
    const workerQueue = mockQueue((batch, options) => {
      settledByBatch.set(batch, () => options?.onSettled?.(batch));
    });
    return { workerQueue, settle: (batch) => settledByBatch.get(batch)?.() };
  }

  const pushedBatches = (workerQueue: ReturnType<typeof mockQueue>): Batch[] =>
    workerQueue.pushForNode.mock.calls.flatMap(([, batches]) => batches as Batch[]);

  it('holds a batch until the earlier batch of its partition settles, then enqueues it', async () => {
    const first = { topic: 'orders', partition: 0 };
    const second = { topic: 'orders', partition: 0 };
    const other = { topic: 'orders', partition: 1 };
    const { fetch, release } = fetchResponses([[first], [second, other]]);
    const { workerQueue, settle } = batchSettler();
    const fetcher = createFetcher({
      nodeId: '1',
      workerQueue,
      partitionAssignments: new Map(),
      fetch,
      logger: silentLogger,
    });

    const started = fetcher.start();
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledTimes(3));
    expect(pushedBatches(workerQueue)).toEqual([first, other]);

    settle(first);
    await vi.waitFor(() => expect(pushedBatches(workerQueue)).toEqual([first, other, second]));
    expect(pushedBatches(workerQueue)[2]).toBe(second);

    const stopping = fetcher.stop();
    release();
    await started;
    await stopping;
  });

  it('keeps fetching other partitions while a slow partition has a held batch', async () => {
    const slowFirst = { topic: 'orders', partition: 0 };
    const slowSecond = { topic: 'orders', partition: 0 };
    const fast = [1, 2, 3].map(() => ({ topic: 'orders', partition: 1 }));
    const { fetch, release } = fetchResponses([[slowFirst], [slowSecond, fast[0]!], [fast[1]!], [fast[2]!]]);
    const settledByBatch = new Map<Batch, () => void>();
    const workerQueue = mockQueue((batch, options) => {
      if (batch.partition === 1) options?.onSettled?.(batch);
      else settledByBatch.set(batch, () => options?.onSettled?.(batch));
    });
    const fetcher = createFetcher({
      nodeId: '1',
      workerQueue,
      partitionAssignments: new Map(),
      fetch,
      logger: silentLogger,
    });

    const started = fetcher.start();
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledTimes(5));
    expect(pushedBatches(workerQueue)).toEqual([slowFirst, ...fast]);

    settledByBatch.get(slowFirst)?.();
    await vi.waitFor(() => expect(pushedBatches(workerQueue)).toHaveLength(5));
    expect(pushedBatches(workerQueue)[4]).toBe(slowSecond);

    const stopping = fetcher.stop();
    release();
    await started;
    await stopping;
  });

  it('drops a held batch that is no longer current once its partition frees up', async () => {
    const first = { topic: 'orders', partition: 0 };
    const second = { topic: 'orders', partition: 0 };
    const { fetch, release } = fetchResponses([[first], [second]]);
    let isPaused = false;
    const onSecondSettled = vi.fn();
    trackFetchBatch(second, { isCurrent: () => !isPaused, onSettled: onSecondSettled });
    const { workerQueue, settle } = batchSettler();
    const fetcher = createFetcher({
      nodeId: '1',
      workerQueue,
      partitionAssignments: new Map(),
      fetch,
      logger: silentLogger,
    });

    const started = fetcher.start();
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledTimes(3));
    isPaused = true;
    settle(first);
    expect(onSecondSettled).toHaveBeenCalledTimes(1);
    expect(workerQueue.pushForNode).toHaveBeenCalledTimes(1);

    const stopping = fetcher.stop();
    release();
    await started;
    await stopping;
  });

  it('settles a held batch without enqueueing it when stopped', async () => {
    const first = { topic: 'orders', partition: 0 };
    const second = { topic: 'orders', partition: 0 };
    const { fetch, release } = fetchResponses([[first], [second]]);
    const onSecondSettled = vi.fn();
    trackFetchBatch(second, { isCurrent: () => true, onSettled: onSecondSettled });
    const { workerQueue, settle } = batchSettler();
    const partitionAssignments = new Map<string, string>();
    const fetcher = createFetcher({
      nodeId: '1',
      workerQueue,
      partitionAssignments,
      fetch,
      logger: silentLogger,
    });

    const started = fetcher.start();
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledTimes(3));
    const stopping = fetcher.stop();
    expect(onSecondSettled).toHaveBeenCalledTimes(1);
    release();
    await started;
    await stopping;

    settle(first);
    expect(workerQueue.pushForNode).toHaveBeenCalledTimes(1);
    expect(partitionAssignments.has('orders|0')).toBe(false);
  });

  it('runs the settle callback when the worker queue settles an enqueued batch', async () => {
    const batch = { topic: 'orders', partition: 0 };
    const { fetch, release } = fetchResponses([[batch]]);
    const onSettled = vi.fn();
    trackFetchBatch(batch, { isCurrent: () => true, onSettled });
    const workerQueue = mockQueue((enqueued, options) => options?.onSettled?.(enqueued));
    const partitionAssignments = new Map<string, string>();
    const fetcher = createFetcher({
      nodeId: '1',
      workerQueue,
      partitionAssignments,
      fetch,
      logger: silentLogger,
    });

    const started = fetcher.start();
    await vi.waitFor(() => expect(onSettled).toHaveBeenCalledTimes(1));
    expect(partitionAssignments.has('orders|0')).toBe(false);

    const stopping = fetcher.stop();
    release();
    await started;
    await stopping;
  });

  it('filters batches already assigned to another fetcher', async () => {
    const gate = deferred<Batch[]>();
    const fetch = vi
      .fn()
      .mockResolvedValueOnce([
        { topic: 'orders', partition: 0 },
        { topic: 'orders', partition: 1 },
      ])
      .mockImplementation(() => gate.promise);
    const workerQueue = mockQueue();
    const partitionAssignments = new Map<string, string>([['orders|0', 'other']]);
    const fetcher = createFetcher({
      nodeId: '1',
      workerQueue,
      partitionAssignments,
      fetch,
      logger: silentLogger,
    });

    const started = fetcher.start();
    await vi.waitFor(() => expect(workerQueue.pushForNode).toHaveBeenCalled());
    expect(workerQueue.pushForNode).toHaveBeenCalledWith('1', [{ topic: 'orders', partition: 1 }], expect.any(Object));
    expect(partitionAssignments.get('orders|1')).toBe('1');

    const stopping = fetcher.stop();
    gate.resolve([]);
    await started;
    await stopping;
  });

  it('unassigns batches that were not enqueued when pushForNode fails', async () => {
    const fetch = vi.fn().mockResolvedValue([
      { topic: 'orders', partition: 0 },
      { topic: 'orders', partition: 1 },
    ]);
    const workerQueue = {
      pushForNode: vi.fn(
        async (_nodeId: string, batches: Batch[], options?: { onEnqueued?: (batch: Batch) => void }) => {
          options?.onEnqueued?.(batches[0]!);
          throw new Error('queue full');
        },
      ),
      interrupt: vi.fn(),
    } as unknown as WorkerQueue<Batch>;
    const partitionAssignments = new Map<string, string>();
    const fetcher = createFetcher({
      nodeId: '1',
      workerQueue,
      partitionAssignments,
      fetch,
      logger: silentLogger,
    });

    await expect(fetcher.start()).rejects.toThrow('queue full');
    expect(partitionAssignments.get('orders|0')).toBe('1');
    expect(partitionAssignments.has('orders|1')).toBe(false);
  });

  it('stops a running fetch loop and interrupts the worker queue', async () => {
    const gate = deferred<Batch[]>();
    const fetch = vi.fn(() => gate.promise);
    const workerQueue = mockQueue();
    const fetcher = createFetcher({
      nodeId: '1',
      workerQueue,
      partitionAssignments: new Map(),
      fetch,
      logger: silentLogger,
    });

    const started = fetcher.start();
    await vi.waitFor(() => expect(fetch).toHaveBeenCalled());
    const stopping = fetcher.stop();
    expect(workerQueue.interrupt).toHaveBeenCalled();
    gate.resolve([]);
    await started;
    await stopping;
    expect(fetcher.getWorkerQueue()).toBe(workerQueue);
  });
});
