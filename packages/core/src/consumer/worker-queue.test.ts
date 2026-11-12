import { describe, expect, it, vi } from 'vitest';
import { TIMESTAMP_TYPES } from '../protocol/enums/timestamp-types';
import { Batch } from './batch';
import { seq } from '../utils/seq';
import { sleep } from '../utils/wait';
import type { KafkaMessage } from './types';
import { createWorker } from './worker';
import { createWorkerQueue, estimatePrefetchBytes } from './worker-queue';

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

function kafkaMessage(offset: bigint, valueSize = 0): KafkaMessage {
  return {
    magicByte: 2,
    attributes: 0,
    timestamp: 0n,
    offset,
    key: null,
    value: valueSize > 0 ? Buffer.alloc(valueSize) : Buffer.from('x'),
    headers: {},
    isControlRecord: false,
    batchContext: defaultBatchContext,
    byteSize: valueSize > 0 ? valueSize : 1,
  };
}

const createBatch = (partition: number, messages: KafkaMessage[] = []): Batch =>
  new Batch('test-topic', 0n, {
    partition,
    highWatermark: 100n,
    messages,
  });

describe('consumer/worker-queue prefetch', () => {
  it('resolves push before the handler finishes', async () => {
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const handler = vi.fn(async (_batch: Batch) => gate);
    const workers = seq(1, (workerId) => createWorker({ handler, workerId }));
    const workerQueue = createWorkerQueue<Batch>({ workers, maxBatchesPerNode: 8 });

    await workerQueue.push(createBatch(0, [kafkaMessage(0n)]));
    expect(handler).toHaveBeenCalledTimes(1);
    expect(workerQueue.buffered().batches).toBeGreaterThan(0);

    release();
    await workerQueue.idle();
    expect(workerQueue.buffered().batches).toBe(0);
  });

  it('pauses enqueue when the per-node batch cap is full', async () => {
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const handler = vi.fn(async (_batch: Batch) => gate);
    const workers = seq(1, (workerId) => createWorker({ handler, workerId }));
    const workerQueue = createWorkerQueue<Batch>({ workers, maxBatchesPerNode: 2, maxBytesPerNode: 1_000_000 });

    const first = workerQueue.pushForNode('1', [createBatch(0), createBatch(1)]);
    await first;
    expect(workerQueue.buffered().batches).toBe(2);

    let secondResolved = false;
    const second = workerQueue.pushForNode('1', [createBatch(2)]).then(() => {
      secondResolved = true;
    });
    await sleep(20);
    expect(secondResolved).toBe(false);

    release();
    await second;
    await workerQueue.idle();
    expect(handler).toHaveBeenCalledTimes(3);
  });

  it('discards queued prefetch on seek/stale without running the handler', async () => {
    const stale = new Set<number>([1]);
    const handler = vi.fn(async (_batch: Batch) => {
      await sleep(30);
    });
    const workers = seq(1, (workerId) => createWorker({ handler, workerId }));
    const workerQueue = createWorkerQueue<Batch>({
      workers,
      maxBatchesPerNode: 8,
      isStale: (batch) => stale.has(batch.partition),
    });

    await workerQueue.push(createBatch(0), createBatch(1), createBatch(2));
    const dropped = workerQueue.discardQueued((batch) => batch.partition === 1);
    expect(dropped).toBeGreaterThanOrEqual(0);
    await workerQueue.idle();

    const partitions = handler.mock.calls.map(([batch]) => batch.partition);
    expect(partitions).not.toContain(1);
    expect(partitions).toEqual(expect.arrayContaining([0, 2]));
  });

  it('drops stale batches at dequeue when seek lands after enqueue', async () => {
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const stale = new Set<number>();
    const handler = vi.fn(async (batch: Batch) => {
      if (batch.partition === 0) await gate;
    });
    const workers = seq(1, (workerId) => createWorker({ handler, workerId }));
    const workerQueue = createWorkerQueue<Batch>({
      workers,
      maxBatchesPerNode: 8,
      isStale: (batch) => stale.has(batch.partition),
    });

    await workerQueue.push(createBatch(0), createBatch(1));
    stale.add(1);
    release();
    await workerQueue.idle();

    const partitions = handler.mock.calls.map(([batch]) => batch.partition);
    expect(partitions).toEqual([0]);
  });

  it('estimates prefetch bytes from each message byteSize plus its length prefix', () => {
    expect(estimatePrefetchBytes({ messages: [] })).toBe(1);
    expect(estimatePrefetchBytes({ messages: [{ byteSize: 10 }, { byteSize: 5 }] })).toBe(17);
    expect(estimatePrefetchBytes({ messages: [{ byteSize: 64 }] })).toBe(66);
  });

  it('adds one record batch header per distinct batchContext', () => {
    const first = { ...defaultBatchContext };
    const second = { ...defaultBatchContext };
    const messages = [
      { byteSize: 123, batchContext: first },
      { byteSize: 123, batchContext: first },
      { byteSize: 123, batchContext: second },
    ];

    expect(estimatePrefetchBytes({ messages })).toBe(3 * (123 + 2) + 2 * 61);
  });

  it('sizes a batch from its raw messages once and reuses the result', () => {
    const batch = new Batch('test-topic', 1n, {
      partition: 0,
      highWatermark: 100n,
      messages: [kafkaMessage(0n, 10), kafkaMessage(1n, 10)],
    });
    const expected = 2 * (10 + 1) + 61;

    expect(estimatePrefetchBytes(batch)).toBe(expected);
    batch.rawMessages.forEach((message) => {
      (message as { byteSize: number }).byteSize = 1_000;
    });
    expect(estimatePrefetchBytes(batch)).toBe(expected);
  });
});
