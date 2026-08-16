import { sharedPromiseTo } from '../utils/shared-promise-to.js';

export type WorkerHandler<T> = (batch: T, metadata: { workerId: number }) => Promise<void>;

export interface QueueItem<T> {
  batch: T;
  resolve: () => void;
  reject: (error: unknown) => void;
}

export interface Worker<T> {
  run: (options: { next: () => QueueItem<T> | undefined }) => Promise<void>;
}

export function createWorker<T>({ handler, workerId }: { handler: WorkerHandler<T>; workerId: number }): Worker<T> {
  const run = sharedPromiseTo(async ({ next }: { next: () => QueueItem<T> | undefined }): Promise<void> => {
    while (true) {
      const item = next();
      if (!item) break;

      const { batch, resolve, reject } = item;
      try {
        await handler(batch, { workerId });
        resolve();
      } catch (error) {
        reject(error);
      }
    }
  });

  return { run };
}
