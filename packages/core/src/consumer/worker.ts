export type WorkerHandler<T> = (batch: T, metadata: { workerId: number }) => Promise<void>;

export interface QueueItem<T> {
  batch: T;
  resolve: () => void;
  reject: (error: unknown) => void;
}

export interface WorkerRunOptions<T> {
  next: () => QueueItem<T> | undefined;
  /** Resolves true when an item may be available; false when the worker should exit. */
  wait?: () => Promise<boolean>;
}

export interface Worker<T> {
  run: (options: WorkerRunOptions<T>) => Promise<void>;
}

export function createWorker<T>({ handler, workerId }: { handler: WorkerHandler<T>; workerId: number }): Worker<T> {
  let draining = false;
  let restart = false;

  const run = async ({ next, wait }: WorkerRunOptions<T>): Promise<void> => {
    if (draining) {
      restart = true;
      return;
    }
    draining = true;
    try {
      do {
        restart = false;
        while (true) {
          let item = next();
          if (!item && wait) {
            const hasWork = await wait();
            if (!hasWork) break;
            item = next();
          }
          if (!item) break;

          const { batch, resolve, reject } = item;
          try {
            await handler(batch, { workerId });
            resolve();
          } catch (error) {
            reject(error);
          }
        }
      } while (restart);
    } finally {
      draining = false;
      if (restart) {
        restart = false;
        void run({ next, wait });
      }
    }
  };

  return { run };
}
