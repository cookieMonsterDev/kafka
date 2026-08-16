import { describe, expect, it, vi } from 'vitest';
import { TIMESTAMP_TYPES } from '../protocol/enums/timestamp-types.js';
import { Batch } from './batch.js';
import { seq } from '../utils/seq.js';
import type { KafkaMessage } from './types.js';
import { createWorker } from './worker.js';
import { createWorkerQueue } from './worker-queue.js';

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
    key: null,
    value: null,
    headers: {},
    isControlRecord: false,
    batchContext: defaultBatchContext,
  };
}

const createBatch = (partition: number): Batch =>
  new Batch('test-topic', 0n, {
    partition,
    highWatermark: 100n,
    messages: seq(6, (i) => kafkaMessage(BigInt(i))),
  });

describe('consumer/worker', () => {
  it('loops until next() returns undefined', async () => {
    const resolve = vi.fn();
    const reject = vi.fn();
    const handler = vi.fn(async () => {});
    const [first, second] = seq(2).map(createBatch);
    const next = vi
      .fn()
      .mockImplementationOnce(() => ({ batch: first, resolve, reject }))
      .mockImplementationOnce(() => ({ batch: second, resolve, reject }));

    const worker = createWorker({ handler, workerId: 0 });
    await worker.run({ next });

    expect(next).toHaveBeenCalledTimes(3);
    expect(handler).toHaveBeenCalledTimes(2);
    expect(handler).toHaveBeenCalledWith(first, { workerId: 0 });
    expect(handler).toHaveBeenCalledWith(second, { workerId: 0 });
    expect(resolve).toHaveBeenCalledTimes(2);
  });

  it('propagates handler exceptions via reject', async () => {
    const resolve = vi.fn();
    const reject = vi.fn();
    const error = new Error('test');
    const handler = vi.fn(async () => {
      throw error;
    });
    const next = vi.fn().mockImplementationOnce(() => ({ batch: 'first', resolve, reject }));
    const worker = createWorker({ handler, workerId: 0 });

    await worker.run({ next });
    expect(reject).toHaveBeenCalledWith(error);
  });
});

describe('consumer/worker-queue', () => {
  const batches = seq(
    100,
    (index) => new Batch('test-topic', 0n, { partition: index, highWatermark: 100n, messages: [] }),
  );

  it('handles all messages within one push', async () => {
    const handler = vi.fn(async () => {});
    const workers = seq(3, (workerId) => createWorker({ handler, workerId }));
    const workerQueue = createWorkerQueue({ workers });
    await workerQueue.push(...batches);
    expect(handler).toHaveBeenCalledTimes(100);
  });

  it('finishes processing before throwing an exception', async () => {
    const handler = vi.fn(async () => {});
    handler.mockImplementationOnce(async () => {
      throw new Error('test');
    });
    const workers = seq(3, (workerId) => createWorker({ handler, workerId }));
    const workerQueue = createWorkerQueue({ workers });
    await expect(workerQueue.push(...batches)).rejects.toThrow('test');
    expect(handler).toHaveBeenCalledTimes(100);
  });
});
