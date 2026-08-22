import { afterEach, describe, expect, it, vi } from 'vitest';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { KafkaNoBrokerAvailableError, KafkaNonRetriableError } from '../errors';
import { seq } from '../utils/seq';
import { sleep, waitFor } from '../utils/wait';
import { Batch } from './batch';
import { createFetchManager, type FetchManager } from './fetch-manager';
import { createFetcher } from './fetcher';
import { createWorker } from './worker';
import { createWorkerQueue } from './worker-queue';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

describe('consumer/fetch-manager', () => {
  let fetchManager: FetchManager<Batch> | undefined;

  afterEach(async () => {
    if (fetchManager) await fetchManager.stop();
  });

  it('constructs fetchers and workers', async () => {
    const getNodeIds = vi.fn(() => seq(4, (i) => String(i)));
    const fetch = vi.fn(async (nodeId: string) =>
      seq(
        10,
        (id) => new Batch('test-topic', 0n, { partition: Number(`${nodeId}${id}`), highWatermark: 100n, messages: [] }),
      ),
    );
    const handler = vi.fn(async () => {
      await sleep(20);
    });

    const manager = createFetchManager({ logger: silentLogger, concurrency: 3, fetch, handler, getNodeIds });
    fetchManager = manager;
    void manager.start();

    const fetchers = manager.getFetchers();
    expect(fetchers).toHaveLength(getNodeIds().length);
    expect(fetchers[0]!.getWorkerQueue().getWorkers()).toHaveLength(3);
  });

  it('finishes processing other batches in case of an error from any single worker', async () => {
    const getNodeIds = vi.fn(() => seq(4, (i) => String(i)));
    const batchSize = 10;
    const fetched = new Set<string>();
    const fetch = vi.fn(async (nodeId: string) => {
      if (fetched.has(nodeId)) {
        await sleep(10_000);
        return [];
      }
      fetched.add(nodeId);
      return seq(
        batchSize,
        (id) => new Batch('test-topic', 0n, { partition: Number(`${nodeId}${id}`), highWatermark: 100n, messages: [] }),
      );
    });
    const handler = vi.fn(async () => {
      await sleep(20);
    });
    handler.mockImplementationOnce(async () => {
      throw new Error('test');
    });

    const manager = createFetchManager({
      logger: silentLogger,
      concurrency: 3,
      prefetchMaxBatches: 50,
      fetch,
      handler,
      getNodeIds,
    });
    fetchManager = manager;
    await expect(manager.start()).rejects.toThrow('test');
    expect(handler).toHaveBeenCalledTimes(getNodeIds().length * batchSize);
  });

  it('rebalances fetchers when nodeIds change', async () => {
    const getNodeIds = vi.fn(() => seq(2, (i) => String(i)));
    const fetch = vi.fn(async () => {
      await sleep(1);
      return [new Batch('test-topic', 0n, { partition: 0, highWatermark: 100n, messages: [] })];
    });
    const handler = vi.fn(async () => {
      await sleep(20);
    });

    const manager = createFetchManager({ logger: silentLogger, concurrency: 3, fetch, handler, getNodeIds });
    fetchManager = manager;
    void manager.start();
    expect(manager.getFetchers()).toHaveLength(2);

    getNodeIds.mockImplementation(() => seq(3, (i) => String(i)));
    fetch.mockClear();
    await waitFor(() => fetch.mock.calls.length > 0);
    expect(manager.getFetchers()).toHaveLength(3);
  });

  it('does not rebalance when all brokers become unavailable; the error bubbles up', async () => {
    const getNodeIds = vi.fn(() => seq(1, (i) => String(i)));
    const realFetch = vi.fn(async () => [
      new Batch('test-topic', 0n, { partition: 0, highWatermark: 100n, messages: [] }),
    ]);
    const fetch = vi.fn(async (nodeId: string) => {
      if (!getNodeIds().includes(nodeId)) {
        throw new KafkaNonRetriableError('Node not found');
      }
      return realFetch();
    });
    const handler = vi.fn(async () => {
      await sleep(20);
    });

    const manager = createFetchManager({ logger: silentLogger, concurrency: 1, fetch, handler, getNodeIds });
    fetchManager = manager;
    const startPromise = manager.start();
    expect(manager.getFetchers()).toHaveLength(1);

    getNodeIds.mockImplementation(() => []);
    await expect(startPromise).rejects.toThrow('Node not found');
  });

  it('throws when there are no brokers available', async () => {
    const manager = createFetchManager<Batch>({
      logger: silentLogger,
      concurrency: 1,
      fetch: vi.fn(async () => []),
      handler: vi.fn(async () => {}),
      getNodeIds: vi.fn(() => []),
    });
    fetchManager = manager;
    await expect(manager.start()).rejects.toThrow(KafkaNoBrokerAvailableError);
  });

  it('discards prefetched batches when fetchers rebalance', async () => {
    const getNodeIds = vi.fn(() => ['0']);
    let releaseHandler: () => void = () => {};
    const handlerGate = new Promise<void>((resolve) => {
      releaseHandler = resolve;
    });
    const fetch = vi.fn(async () => {
      await sleep(5);
      return seq(4, (partition) => new Batch('test-topic', 0n, { partition, highWatermark: 100n, messages: [] }));
    });
    const handler = vi.fn(async () => handlerGate);

    const manager = createFetchManager({
      logger: silentLogger,
      concurrency: 1,
      prefetchMaxBatches: 8,
      fetch,
      handler,
      getNodeIds,
    });
    fetchManager = manager;
    void manager.start();
    await waitFor(() => handler.mock.calls.length > 0);

    getNodeIds.mockImplementation(() => ['0', '1']);
    releaseHandler();
    await waitFor(() => manager.getFetchers().length === 2);
    expect(manager.getFetchers()).toHaveLength(2);
  });
});

describe('consumer/fetcher', () => {
  it('fetches but does not push to the worker queue before exiting', async () => {
    const fetch = vi.fn(async () => {
      await sleep(1);
      return seq(10, (index) => new Batch('test-topic', 0n, { partition: index, highWatermark: 100n, messages: [] }));
    });
    const handler = vi.fn(async () => {
      await sleep(1);
    });
    const workers = seq(5, (workerId) => createWorker({ handler, workerId }));
    const workerQueue = createWorkerQueue<Batch>({ workers });
    const fetcher = createFetcher({
      nodeId: '0',
      fetch,
      workerQueue,
      logger: silentLogger,
      partitionAssignments: new Map(),
    });

    void fetcher.start();
    await fetcher.stop();

    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch).toHaveBeenCalledWith('0');
    expect(handler).toHaveBeenCalledTimes(0);
  });

  it('utilizes all workers', async () => {
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const fetch = vi.fn(async () => {
      await sleep(1);
      return seq(10, (index) => new Batch('test-topic', 0n, { partition: index, highWatermark: 100n, messages: [] }));
    });
    const handler = vi.fn(async (_batch: Batch, _meta: { workerId: number }) => {
      await gate;
    });
    const workerIds = seq(5);
    const workers = workerIds.map((workerId) => createWorker({ handler, workerId }));
    const workerQueue = createWorkerQueue<Batch>({ workers, maxBatchesPerNode: 16 });
    const fetcher = createFetcher({
      nodeId: '0',
      fetch,
      workerQueue,
      logger: silentLogger,
      partitionAssignments: new Map(),
    });

    void fetcher.start();
    await waitFor(() => handler.mock.calls.length >= workerIds.length);
    const calledWorkerIds = handler.mock.calls.map(([, extra]) => extra.workerId);
    for (const workerId of workerIds) {
      expect(calledWorkerIds).toContain(workerId);
    }
    release();
    await fetcher.stop();
  });

  it('is a no-op when start is called while already running', async () => {
    const fetch = vi.fn(async () => {
      await sleep(20);
      return [new Batch('test-topic', 0n, { partition: 0, highWatermark: 100n, messages: [] })];
    });
    const handler = vi.fn(async () => {
      await sleep(1);
    });
    const workers = seq(1, (workerId) => createWorker({ handler, workerId }));
    const workerQueue = createWorkerQueue<Batch>({ workers });
    const fetcher = createFetcher({
      nodeId: '0',
      fetch,
      workerQueue,
      logger: silentLogger,
      partitionAssignments: new Map(),
    });

    void fetcher.start();
    await waitFor(() => fetch.mock.calls.length > 0);
    await expect(fetcher.start()).resolves.toBeUndefined();
    await fetcher.stop();
  });

  it('is a no-op when stop is called while not running', async () => {
    const fetcher = createFetcher({
      nodeId: '0',
      fetch: vi.fn(async () => []),
      workerQueue: createWorkerQueue<Batch>({ workers: [] }),
      logger: silentLogger,
      partitionAssignments: new Map(),
    });

    await expect(fetcher.stop()).resolves.toBeUndefined();
  });

  it('propagates a fetch error and stops the loop', async () => {
    const fetch = vi.fn(async () => {
      throw new Error('broker down');
    });
    const fetcher = createFetcher({
      nodeId: '0',
      fetch,
      workerQueue: createWorkerQueue<Batch>({ workers: [] }),
      logger: silentLogger,
      partitionAssignments: new Map(),
    });

    await expect(fetcher.start()).rejects.toThrow('broker down');
  });

  it('drops batches already assigned to another fetcher', async () => {
    const partitionAssignments = new Map([['test-topic|0', 'other']]);
    const fetch = vi.fn(async () => {
      await sleep(5);
      return [
        new Batch('test-topic', 0n, { partition: 0, highWatermark: 100n, messages: [] }),
        new Batch('test-topic', 0n, { partition: 1, highWatermark: 100n, messages: [] }),
      ];
    });
    const handler = vi.fn(async (_batch: Batch) => {});
    const workers = seq(1, (workerId) => createWorker({ handler, workerId }));
    const workerQueue = createWorkerQueue<Batch>({ workers });
    const fetcher = createFetcher({
      nodeId: '0',
      fetch,
      workerQueue,
      logger: silentLogger,
      partitionAssignments,
    });

    void fetcher.start();
    await waitFor(() => handler.mock.calls.length > 0);
    await fetcher.stop();

    const partitions = handler.mock.calls.map(([batch]) => batch?.partition);
    expect(partitions).toContain(1);
    expect(partitions).not.toContain(0);
  });

  it('does not push when fetch returns no batches', async () => {
    const fetch = vi.fn(async () => {
      await sleep(5);
      return [];
    });
    const handler = vi.fn(async () => {});
    const workers = seq(1, (workerId) => createWorker({ handler, workerId }));
    const workerQueue = createWorkerQueue<Batch>({ workers });
    const fetcher = createFetcher({
      nodeId: '0',
      fetch,
      workerQueue,
      logger: silentLogger,
      partitionAssignments: new Map(),
    });

    void fetcher.start();
    await waitFor(() => fetch.mock.calls.length > 2);
    await fetcher.stop();
    expect(handler).not.toHaveBeenCalled();
  });

  it('does not apply prefetched batches after a seek makes them stale', async () => {
    const seeked = new Set<number>();
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const fetch = vi.fn(async () => {
      await sleep(1);
      return [
        new Batch('test-topic', 0n, { partition: 0, highWatermark: 100n, messages: [] }),
        new Batch('test-topic', 0n, { partition: 1, highWatermark: 100n, messages: [] }),
      ];
    });
    const handler = vi.fn(async (batch: Batch) => {
      if (batch.partition === 0) await gate;
    });
    const workers = seq(1, (workerId) => createWorker({ handler, workerId }));
    const workerQueue = createWorkerQueue<Batch>({
      workers,
      maxBatchesPerNode: 8,
      isStale: (batch) => seeked.has(batch.partition),
    });
    const fetcher = createFetcher({
      nodeId: '0',
      fetch,
      workerQueue,
      logger: silentLogger,
      partitionAssignments: new Map(),
    });

    void fetcher.start();
    await waitFor(() => handler.mock.calls.length > 0);
    seeked.add(1);
    release();
    await waitFor(() => fetch.mock.calls.length > 1);
    await fetcher.stop();

    const partitions = handler.mock.calls.map(([batch]) => batch.partition);
    expect(partitions).toContain(0);
    expect(partitions).not.toContain(1);
  });
});
