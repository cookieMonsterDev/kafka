import { KafkaFetcherRebalanceError, KafkaNoBrokerAvailableError } from '../errors';
import type { Logger } from '../loggers/index';
import { seq } from '../utils/seq';
import { createFetcher, type Fetcher, type FetchBatch } from './fetcher';
import { createWorker, type WorkerHandler } from './worker';
import {
  createWorkerQueue,
  DEFAULT_PREFETCH_MAX_BATCHES,
  DEFAULT_PREFETCH_MAX_BYTES,
  estimatePrefetchBytes,
} from './worker-queue';

export interface FetchManager<T extends FetchBatch = FetchBatch> {
  start: () => Promise<void>;
  stop: () => Promise<void>;
  getFetchers: () => Fetcher<T>[];
}

export function createFetchManager<T extends FetchBatch>({
  logger: rootLogger,
  getNodeIds,
  fetch,
  handler,
  concurrency = 1,
  prefetchMaxBatches = DEFAULT_PREFETCH_MAX_BATCHES,
  prefetchMaxBytes = DEFAULT_PREFETCH_MAX_BYTES,
  getBatchBytes = estimatePrefetchBytes as (batch: T) => number,
  isStale,
}: {
  logger: Logger;
  getNodeIds: () => string[];
  fetch: (nodeId: string) => Promise<T[]>;
  handler: WorkerHandler<T>;
  concurrency?: number;
  prefetchMaxBatches?: number;
  prefetchMaxBytes?: number;
  getBatchBytes?: (batch: T) => number;
  isStale?: (batch: T) => boolean;
}): FetchManager<T> {
  const logger = rootLogger.namespace('FetchManager');
  const workers = seq(concurrency, (workerId) => createWorker({ handler, workerId }));
  const workerQueue = createWorkerQueue({
    workers,
    maxBatchesPerNode: prefetchMaxBatches,
    maxBytesPerNode: prefetchMaxBytes,
    getBatchBytes,
    isStale,
  });

  let fetchers: Fetcher<T>[] = [];

  const getFetchers = (): Fetcher<T>[] => fetchers;

  const createFetchers = (): Fetcher<T>[] => {
    const nodeIds = getNodeIds();
    const partitionAssignments = new Map<string, string>();

    if (nodeIds.length === 0) {
      throw new KafkaNoBrokerAvailableError();
    }

    const validateShouldRebalance = (): void => {
      const current = getNodeIds();
      const hasChanged = nodeIds.length !== current.length || nodeIds.some((nodeId) => !current.includes(nodeId));
      if (hasChanged && current.length !== 0) {
        throw new KafkaFetcherRebalanceError();
      }
    };

    const nextFetchers = nodeIds.map((nodeId) =>
      createFetcher({
        nodeId,
        workerQueue,
        partitionAssignments,
        fetch: async (id) => {
          validateShouldRebalance();
          return fetch(id);
        },
        logger,
      }),
    );

    logger.debug(`Created ${nextFetchers.length} fetchers`, {
      nodeIds,
      concurrency,
      prefetchMaxBatches,
      prefetchMaxBytes,
    });
    return nextFetchers;
  };

  const stop = async (options: { discard?: boolean } = {}): Promise<void> => {
    logger.debug('Stopping fetchers...');
    workerQueue.interrupt();
    if (options.discard) {
      workerQueue.discardQueued();
    }
    await Promise.all(fetchers.map((fetcher) => fetcher.stop()));
    if (options.discard) {
      workerQueue.discardQueued();
    }
    logger.debug('Stopped fetchers');
  };

  const start = async (): Promise<void> => {
    logger.debug('Starting...');

    while (true) {
      workerQueue.resetFailure();
      fetchers = createFetchers();

      try {
        await Promise.race([Promise.all(fetchers.map((fetcher) => fetcher.start())), workerQueue.failed()]);
      } catch (error) {
        if (error instanceof KafkaFetcherRebalanceError) {
          logger.debug('Rebalancing fetchers...');
          await stop({ discard: true });
          continue;
        }

        await stop();
        await workerQueue.idle();
        throw error;
      }

      break;
    }
  };

  return {
    start,
    stop: async () => {
      await stop();
      await workerQueue.idle();
    },
    getFetchers,
  };
}
