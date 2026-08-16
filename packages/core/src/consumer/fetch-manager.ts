import { KafkaFetcherRebalanceError, KafkaNoBrokerAvailableError } from '../errors';
import type { Logger } from '../loggers/index';
import { seq } from '../utils/seq';
import { createFetcher, type Fetcher, type FetchBatch } from './fetcher';
import { createWorker, type WorkerHandler } from './worker';
import { createWorkerQueue } from './worker-queue';

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
}: {
  logger: Logger;
  getNodeIds: () => string[];
  fetch: (nodeId: string) => Promise<T[]>;
  handler: WorkerHandler<T>;
  concurrency?: number;
}): FetchManager<T> {
  const logger = rootLogger.namespace('FetchManager');
  const workers = seq(concurrency, (workerId) => createWorker({ handler, workerId }));
  const workerQueue = createWorkerQueue({ workers });

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

    logger.debug(`Created ${nextFetchers.length} fetchers`, { nodeIds, concurrency });
    return nextFetchers;
  };

  const stop = async (): Promise<void> => {
    logger.debug('Stopping fetchers...');
    await Promise.all(fetchers.map((fetcher) => fetcher.stop()));
    logger.debug('Stopped fetchers');
  };

  const start = async (): Promise<void> => {
    logger.debug('Starting...');

    while (true) {
      fetchers = createFetchers();

      try {
        await Promise.all(fetchers.map((fetcher) => fetcher.start()));
      } catch (error) {
        await stop();

        if (error instanceof KafkaFetcherRebalanceError) {
          logger.debug('Rebalancing fetchers...');
          continue;
        }

        throw error;
      }

      break;
    }
  };

  return { start, stop, getFetchers };
}
