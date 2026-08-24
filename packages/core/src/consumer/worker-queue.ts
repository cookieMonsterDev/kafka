import type { QueueItem, Worker } from './worker';

/** Default cap on queued+in-flight batches for one fetch node. */
export const DEFAULT_PREFETCH_MAX_BATCHES = 4;
/** Default cap on queued+in-flight record bytes for one fetch node (10 MiB). */
export const DEFAULT_PREFETCH_MAX_BYTES = 10_485_760;

export interface PrefetchSize {
  byteSize: number;
}

/**
 * Lower bound of on-wire bytes in a batch; empty batches count as 1 byte so they still occupy a
 * slot. Sums each message's `byteSize` rather than its decoded `key`/`value` lengths — the
 * latter would force every message's `value`/`headers` to decode just to size the batch,
 * defeating the point of decoding them lazily (see `DecodedRecord`).
 */
export function estimatePrefetchBytes(batch: { messages?: readonly PrefetchSize[] }): number {
  const messages = batch.messages;
  if (messages == null || messages.length === 0) return 1;
  let bytes = 0;
  for (const message of messages) {
    bytes += message.byteSize;
  }
  return bytes > 0 ? bytes : 1;
}

export interface PushForNodeOptions<T> {
  onSettled?: (batch: T) => void;
  onEnqueued?: (batch: T) => void;
}

export interface WorkerQueue<T> {
  push: (...batches: T[]) => Promise<void>;
  pushForNode: (nodeId: string, batches: readonly T[], options?: PushForNodeOptions<T>) => Promise<void>;
  discardQueued: (predicate?: (batch: T) => boolean) => number;
  interrupt: () => void;
  resetFailure: () => void;
  idle: () => Promise<void>;
  failed: () => Promise<never>;
  getWorkers: () => Worker<T>[];
  buffered: () => { batches: number; bytes: number };
}

interface NodeUsage {
  batches: number;
  bytes: number;
}

interface InternalItem<T> extends QueueItem<T> {
  nodeId: string;
  bytes: number;
  markStarted: () => void;
}

export function createWorkerQueue<T>({
  workers,
  maxBatchesPerNode = DEFAULT_PREFETCH_MAX_BATCHES,
  maxBytesPerNode = DEFAULT_PREFETCH_MAX_BYTES,
  getBatchBytes = estimatePrefetchBytes as (batch: T) => number,
  isStale,
}: {
  workers: Worker<T>[];
  maxBatchesPerNode?: number;
  maxBytesPerNode?: number;
  getBatchBytes?: (batch: T) => number;
  isStale?: (batch: T) => boolean;
}): WorkerQueue<T> {
  const asError = (reason: unknown): Error => (reason instanceof Error ? reason : new Error(String(reason)));

  const queue: InternalItem<T>[] = [];
  const usage = new Map<string, NodeUsage>();
  let inFlight = 0;
  let error: Error | undefined;
  let interruptGeneration = 0;
  const errorWaiters: Array<(reason: Error) => void> = [];
  const capacityWaiters: Array<() => void> = [];
  const idleWaiters: Array<() => void> = [];

  const getUsage = (nodeId: string): NodeUsage => {
    const current = usage.get(nodeId);
    if (current) return current;
    const created: NodeUsage = { batches: 0, bytes: 0 };
    usage.set(nodeId, created);
    return created;
  };

  const addUsage = (nodeId: string, bytes: number): void => {
    const current = getUsage(nodeId);
    current.batches += 1;
    current.bytes += bytes;
  };

  const releaseUsage = (nodeId: string, bytes: number): void => {
    const current = usage.get(nodeId);
    if (!current) return;
    current.batches = Math.max(0, current.batches - 1);
    current.bytes = Math.max(0, current.bytes - bytes);
    if (current.batches === 0 && current.bytes === 0) usage.delete(nodeId);
  };

  const wake = (waiters: Array<() => void>): void => {
    if (waiters.length === 0) return;
    const pending = waiters.splice(0, waiters.length);
    for (const resolve of pending) resolve();
  };

  const notifyIdle = (): void => {
    if (queue.length === 0 && inFlight === 0) wake(idleWaiters);
  };

  const fail = (reason: unknown): void => {
    if (error != null) return;
    error = asError(reason);
    wake(capacityWaiters);
    const waiters = errorWaiters.splice(0, errorWaiters.length);
    for (const reject of waiters) reject(error);
  };

  const canAccept = (nodeId: string, bytes: number): boolean => {
    const current = getUsage(nodeId);
    if (current.batches === 0) return true;
    return current.batches < maxBatchesPerNode && current.bytes + bytes <= maxBytesPerNode;
  };

  const waitForCapacity = async (nodeId: string, bytes: number, generation: number): Promise<void> => {
    while (error == null && interruptGeneration === generation && !canAccept(nodeId, bytes)) {
      await new Promise<void>((resolve) => {
        capacityWaiters.push(resolve);
      });
    }
  };

  const finishItem = (
    item: InternalItem<T>,
    started: { value: boolean },
    onSettled: (() => void) | undefined,
    reason?: unknown,
  ): void => {
    if (started.value) {
      inFlight = Math.max(0, inFlight - 1);
      started.value = false;
    }
    releaseUsage(item.nodeId, item.bytes);
    onSettled?.();
    wake(capacityWaiters);
    notifyIdle();
    if (reason != null) fail(reason);
  };

  const dequeue = (): InternalItem<T> | undefined => {
    while (queue.length > 0) {
      const item = queue.shift();
      if (item == null) return undefined;
      if (isStale?.(item.batch)) {
        item.resolve();
        continue;
      }
      inFlight += 1;
      item.markStarted();
      return item;
    }
    return undefined;
  };

  const startWorkers = (): void => {
    for (const worker of workers) {
      void worker.run({
        next: () => dequeue(),
      });
    }
  };

  const pushForNode = async (nodeId: string, batches: readonly T[], options?: PushForNodeOptions<T>): Promise<void> => {
    if (batches.length === 0) return;
    if (error != null) throw asError(error);

    const generation = interruptGeneration;
    for (const batch of batches) {
      if (interruptGeneration !== generation) return;
      const bytes = Math.max(1, getBatchBytes(batch));
      if (error == null) {
        await waitForCapacity(nodeId, bytes, generation);
      }
      if (interruptGeneration !== generation) return;

      addUsage(nodeId, bytes);
      const started = { value: false };
      const item: InternalItem<T> = {
        batch,
        nodeId,
        bytes,
        markStarted: () => {
          started.value = true;
        },
        resolve: () => {
          finishItem(item, started, () => options?.onSettled?.(batch));
        },
        reject: (reason) => {
          finishItem(item, started, () => options?.onSettled?.(batch), reason);
        },
      };
      queue.push(item);
      options?.onEnqueued?.(batch);
      startWorkers();
    }
    if (error != null) throw asError(error);
  };

  const push = async (...batches: T[]): Promise<void> => {
    await pushForNode('default', batches);
  };

  const discardQueued = (predicate?: (batch: T) => boolean): number => {
    const kept: InternalItem<T>[] = [];
    let dropped = 0;
    for (const item of queue) {
      if (predicate == null || predicate(item.batch)) {
        dropped += 1;
        item.resolve();
      } else {
        kept.push(item);
      }
    }
    queue.length = 0;
    queue.push(...kept);
    notifyIdle();
    return dropped;
  };

  const interrupt = (): void => {
    interruptGeneration += 1;
    errorWaiters.length = 0;
    wake(capacityWaiters);
  };

  const resetFailure = (): void => {
    error = undefined;
  };

  const idle = async (): Promise<void> => {
    while (queue.length > 0 || inFlight > 0) {
      await new Promise<void>((resolve) => {
        idleWaiters.push(resolve);
      });
    }
  };

  const failed = (): Promise<never> =>
    new Promise((_, reject) => {
      if (error != null) {
        reject(error);
        return;
      }
      errorWaiters.push(reject);
    });

  const getWorkers = (): Worker<T>[] => workers;

  const buffered = (): { batches: number; bytes: number } => {
    let batches = 0;
    let bytes = 0;
    for (const current of usage.values()) {
      batches += current.batches;
      bytes += current.bytes;
    }
    return { batches, bytes };
  };

  return { push, pushForNode, discardQueued, interrupt, resetFailure, idle, failed, getWorkers, buffered };
}
