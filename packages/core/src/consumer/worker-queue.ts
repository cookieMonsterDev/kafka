import type { QueueItem, Worker } from './worker.js';

export interface WorkerQueue<T> {
  push: (...batches: T[]) => Promise<void>;
  getWorkers: () => Worker<T>[];
}

export function createWorkerQueue<T>({ workers }: { workers: Worker<T>[] }): WorkerQueue<T> {
  const queue: QueueItem<T>[] = [];

  const getWorkers = (): Worker<T>[] => workers;

  const push = async (...batches: T[]): Promise<void> => {
    const promises = batches.map(
      (batch) =>
        new Promise<void>((resolve, reject) => {
          queue.push({ batch, resolve, reject });
        }),
    );

    for (const worker of workers) {
      void worker.run({ next: () => queue.shift() });
    }

    const results = await Promise.allSettled(promises);
    const rejected = results.find((result): result is PromiseRejectedResult => result.status === 'rejected');
    if (rejected) {
      throw rejected.reason;
    }
  };

  return { push, getWorkers };
}
