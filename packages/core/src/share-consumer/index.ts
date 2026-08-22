import type { Cluster } from '../cluster/index';
import type { ConsumerRetryOptions, EachMessageHandler } from '../consumer/types';
import { KafkaNonRetriableError } from '../errors';
import type { Logger } from '../loggers/index';
import { SHARE_ACQUIRE_MODE, type ShareAcquireMode } from '../protocol/requests/share-fetch/index';
import { RETRY_DEFAULTS } from '../retry/defaults';
import { abortError, rejectOnAbort, type ConnectOptions } from '../utils/abort';
import { ShareGroup } from './share-group';
import { ShareRunner, type EachShareBatchHandler } from './share-runner';

export interface ShareConsumerSubscribeTopics {
  topics: readonly string[];
}

export interface ShareConsumerRunConfig {
  eachMessage?: EachMessageHandler | null;
  eachBatch?: EachShareBatchHandler | null;
  eachBatchAutoAck?: boolean;
  partitionsConsumedConcurrently?: number;
  prefetchMaxBatches?: number;
  prefetchMaxBytes?: number;
}

export interface ShareConsumerOptions {
  cluster: Cluster;
  groupId: string;
  logger: Logger;
  heartbeatInterval?: number;
  maxWaitTimeInMs?: number;
  minBytes?: number;
  maxBytes?: number;
  maxRecords?: number;
  batchSize?: number;
  shareAcquireMode?: ShareAcquireMode;
  rackId?: string;
  retry?: ConsumerRetryOptions;
}

export interface ShareConsumer {
  connect: (options?: ConnectOptions) => Promise<void>;
  disconnect: (options?: ConnectOptions) => Promise<void>;
  subscribe: (subscription: ShareConsumerSubscribeTopics) => void;
  run: (config: ShareConsumerRunConfig) => Promise<void>;
  stop: () => Promise<void>;
  logger: () => Logger;
  [Symbol.asyncDispose]: () => Promise<void>;
}

export function createShareConsumer({
  cluster,
  groupId,
  logger: rootLogger,
  heartbeatInterval = 3000,
  maxWaitTimeInMs,
  minBytes,
  maxBytes,
  maxRecords,
  batchSize,
  shareAcquireMode = SHARE_ACQUIRE_MODE.BATCH_OPTIMIZED,
  rackId,
  retry = RETRY_DEFAULTS,
}: ShareConsumerOptions): ShareConsumer {
  const logger = rootLogger.namespace('ShareConsumer');
  const shareGroup = new ShareGroup({ cluster, groupId, logger, retry, rackId });
  let runner: ShareRunner | null = null;

  async function connect({ signal }: ConnectOptions = {}): Promise<void> {
    if (signal?.aborted) throw abortError(signal);
    await rejectOnAbort(cluster.connect(), signal);
  }

  async function disconnect({ signal }: ConnectOptions = {}): Promise<void> {
    if (signal?.aborted) throw abortError(signal);
    if (runner) await runner.stop();
    await rejectOnAbort(cluster.disconnect(), signal);
  }

  function subscribe({ topics }: ShareConsumerSubscribeTopics): void {
    if (!topics.length) {
      throw new KafkaNonRetriableError('Share consumer subscribe requires at least one topic');
    }
    shareGroup.subscribe(topics);
  }

  async function run({
    eachMessage = null,
    eachBatch = null,
    eachBatchAutoAck = true,
    partitionsConsumedConcurrently = 1,
    prefetchMaxBatches,
    prefetchMaxBytes,
  }: ShareConsumerRunConfig = {}): Promise<void> {
    if (shareGroup.topicsSubscribed.length === 0) {
      throw new KafkaNonRetriableError('Share consumer must subscribe before run()');
    }

    if (!eachMessage && !eachBatch) {
      throw new KafkaNonRetriableError('Share consumer run() requires eachMessage or eachBatch');
    }

    runner = new ShareRunner({
      logger: rootLogger,
      shareGroup,
      eachMessage,
      eachBatch,
      eachBatchAutoAck,
      heartbeatInterval,
      maxWaitTimeInMs,
      minBytes,
      maxBytes,
      maxRecords,
      batchSize,
      shareAcquireMode,
      concurrency: partitionsConsumedConcurrently,
      prefetchMaxBatches,
      prefetchMaxBytes,
      retry,
      onCrash: async (error) => {
        logger.error(`Share consumer crashed: ${error.message}`, { stack: error.stack });
        throw error;
      },
    });

    await runner.start();
  }

  async function stop(): Promise<void> {
    if (runner) await runner.stop();
  }

  return {
    connect,
    disconnect,
    subscribe,
    run,
    stop,
    logger: () => logger,
    [Symbol.asyncDispose]: disconnect,
  };
}

export { SHARE_ACKNOWLEDGE_TYPE } from './acknowledge-types';
export { SHARE_ACQUIRE_MODE } from '../protocol/requests/share-fetch/index';
export type { EachShareBatchHandler, EachShareBatchPayload } from './share-runner';
export { ShareBatch } from './share-batch';
export { ShareGroup } from './share-group';
