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

  const getWorkerQueue = (): WorkerQueue<T> => workerQueue;
  const assignmentKey = ({ topic, partition }: FetchBatch): string => `${topic}|${partition}`;
  const getAssignedFetcher = (batch: FetchBatch): string | undefined => partitionAssignments.get(assignmentKey(batch));
  const assignTopicPartition = (batch: FetchBatch): void => {
    partitionAssignments.set(assignmentKey(batch), nodeId);
  };
  const unassignTopicPartition = (batch: FetchBatch): void => {
    if (getAssignedFetcher(batch) === nodeId) {
      partitionAssignments.delete(assignmentKey(batch));
    }
  };

  const filterUnassignedBatches = (batches: T[]): T[] =>
    batches.filter((batch) => {
      const assignedFetcher = getAssignedFetcher(batch);
      if (assignedFetcher != null && assignedFetcher !== nodeId) {
        logger.info('Filtering out batch due to partition already being processed by another fetcher', {
          topic: batch.topic,
          partition: batch.partition,
          assignedFetcher,
          fetcher: nodeId,
        });
        return false;
      }

      return true;
    });

  const start = async (): Promise<void> => {
    if (isRunning) return;
    isRunning = true;

    while (isRunning) {
      try {
        const batches = await fetch(nodeId);
        if (!isRunning) break;

        const availableBatches = filterUnassignedBatches(batches);
        if (availableBatches.length === 0) {
          await sleep(1);
          continue;
        }

        availableBatches.forEach(assignTopicPartition);
        let enqueued = 0;
        try {
          await workerQueue.pushForNode(nodeId, availableBatches, {
            onEnqueued: () => {
              enqueued += 1;
            },
            onSettled: unassignTopicPartition,
          });
        } catch (error) {
          for (let index = enqueued; index < availableBatches.length; index++) {
            const batch = availableBatches[index];
            if (batch) unassignTopicPartition(batch);
          }
          throw error;
        }
        for (let index = enqueued; index < availableBatches.length; index++) {
          const batch = availableBatches[index];
          if (batch) unassignTopicPartition(batch);
        }
      } catch (error) {
        isRunning = false;
        emitter.emit('end');
        throw error;
      }
    }
    emitter.emit('end');
  };

  const stop = async (): Promise<void> => {
    if (!isRunning) return;
    isRunning = false;
    workerQueue.interrupt();
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
