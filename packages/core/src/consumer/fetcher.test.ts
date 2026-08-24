import { describe, expect, it, vi } from 'vitest';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createFetcher } from './fetcher';
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

function mockQueue() {
  const pushForNode = vi.fn(
    async (_nodeId: string, batches: Batch[], options?: { onEnqueued?: (batch: Batch) => void }) => {
      for (const batch of batches) options?.onEnqueued?.(batch);
    },
  );
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
