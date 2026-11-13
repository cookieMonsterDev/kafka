import { EventEmitter } from 'node:events';
import type { Logger } from '../loggers/index';
import { sleep } from '../utils/wait';
import type { WorkerQueue } from './worker-queue';

export interface FetchBatch {
  topic: string;
  partition: number;
}

export interface Fetcher<T extends FetchBatch> {
  start: () => Promise<void>;
  stop: () => Promise<void>;
  getWorkerQueue: () => WorkerQueue<T>;
}

export interface FetchBatchHooks {
  /** Checked before a held batch is enqueued; `false` drops it, e.g. its partition was paused while it waited. */
  isCurrent: () => boolean;
  /** Runs once the batch leaves the pipeline: handled, dropped as stale, discarded, or never enqueued. */
  onSettled: () => void;
}

const hooksByBatch = new WeakMap<object, FetchBatchHooks>();

export function topicPartitionKey(topic: string, partition: number): string {
  return `${topic}|${partition}`;
}

/** Attaches hooks to a batch returned by a fetch; a later call for the same batch replaces them. */
export function trackFetchBatch(batch: object, hooks: FetchBatchHooks): void {
  hooksByBatch.set(batch, hooks);
}

/** `true` for a batch without hooks. */
export function isFetchBatchCurrent(batch: object): boolean {
  const hooks = hooksByBatch.get(batch);
  return hooks == null || hooks.isCurrent();
}

/** Runs and forgets the batch's `onSettled` hook, if any. */
export function settleFetchBatch(batch: object): void {
  const hooks = hooksByBatch.get(batch);
  if (hooks == null) return;
  hooksByBatch.delete(batch);
  hooks.onSettled();
}

/**
 * Fetches for one node and enqueues the batches. A partition has at most one batch in the worker
 * queue: a batch returned while its partition's earlier batch is still queued or in flight is held
 * (at most one per partition) and enqueued when that earlier batch settles. The fetch callback is
 * expected to leave out partitions that already have a held batch.
 */
export function createFetcher<T extends FetchBatch>({
  nodeId,
  workerQueue,
  partitionAssignments,
  fetch,
  logger: rootLogger,
}: {
  nodeId: string;
  workerQueue: WorkerQueue<T>;
  partitionAssignments: Map<string, string>;
  fetch: (nodeId: string) => Promise<T[]>;
  logger: Logger;
}): Fetcher<T> {
  const logger = rootLogger.namespace(`Fetcher ${nodeId}`);
  const emitter = new EventEmitter();
  let isRunning = false;
  const heldBatches = new Map<string, T>();

  const getWorkerQueue = (): WorkerQueue<T> => workerQueue;
  const assignmentKey = ({ topic, partition }: FetchBatch): string => topicPartitionKey(topic, partition);

  const releaseHeldBatches = (): void => {
    const held = [...heldBatches.values()];
    heldBatches.clear();
    held.forEach(settleFetchBatch);
  };

  const settleAssignedBatch = (batch: T): void => {
    settleFetchBatch(batch);
    const key = assignmentKey(batch);
    if (partitionAssignments.get(key) !== nodeId) return;
    partitionAssignments.delete(key);

    const held = heldBatches.get(key);
    if (held == null) return;
    heldBatches.delete(key);
    if (!isRunning || !isFetchBatchCurrent(held)) {
      settleFetchBatch(held);
      return;
    }
    // A failed push also fails the worker queue, which the fetch manager already watches.
    enqueue([held]).catch((error: unknown) => {
      logger.debug('Failed to enqueue a held batch', { topic: held.topic, partition: held.partition, error });
    });
  };

  const enqueue = async (batches: T[]): Promise<void> => {
    for (const batch of batches) partitionAssignments.set(assignmentKey(batch), nodeId);
    let enqueued = 0;
    try {
      await workerQueue.pushForNode(nodeId, batches, {
        onEnqueued: () => {
          enqueued += 1;
        },
        onSettled: settleAssignedBatch,
      });
    } finally {
      for (let index = enqueued; index < batches.length; index++) {
        const batch = batches[index];
        if (batch) settleAssignedBatch(batch);
      }
    }
  };

  /** Enqueues free partitions' batches and holds the rest; returns how many were enqueued or held. */
  const accept = async (batches: T[]): Promise<number> => {
    const ready: T[] = [];
    let held = 0;
    for (const batch of batches) {
      const key = assignmentKey(batch);
      const assignedFetcher = partitionAssignments.get(key);
      if (assignedFetcher == null) {
        if (isFetchBatchCurrent(batch)) ready.push(batch);
        else settleFetchBatch(batch);
      } else if (assignedFetcher === nodeId && !heldBatches.has(key)) {
        heldBatches.set(key, batch);
        held += 1;
      } else {
        logger.info('Filtering out batch due to partition already being processed', {
          topic: batch.topic,
          partition: batch.partition,
          assignedFetcher,
          fetcher: nodeId,
        });
        settleFetchBatch(batch);
      }
    }
    if (ready.length > 0) await enqueue(ready);
    return ready.length + held;
  };

  const start = async (): Promise<void> => {
    if (isRunning) return;
    isRunning = true;

    while (isRunning) {
      try {
        const batches = await fetch(nodeId);
        if (!isRunning) {
          batches.forEach(settleFetchBatch);
          break;
        }

        const accepted = await accept(batches);
        if (accepted === 0) {
          await sleep(1);
        }
      } catch (error) {
        isRunning = false;
        releaseHeldBatches();
        emitter.emit('end');
        throw error;
      }
    }
    releaseHeldBatches();
    emitter.emit('end');
  };

  const stop = async (): Promise<void> => {
    if (!isRunning) return;
    isRunning = false;
    workerQueue.interrupt();
    releaseHeldBatches();
    await new Promise<void>((resolve) => {
      const timeoutId = setTimeout(resolve, 10_000);
      emitter.once('end', () => {
        clearTimeout(timeoutId);
        resolve();
      });
    });
  };

  return { start, stop, getWorkerQueue };
}
