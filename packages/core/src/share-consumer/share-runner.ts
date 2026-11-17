import { createFetchManager, type FetchManager } from '../consumer/fetch-manager';
import type { EachMessageHandler } from '../consumer/types';
import { KafkaError, KafkaNoBrokerAvailableError } from '../errors';
import { InstrumentationEventEmitter } from '../instrumentation/emitter';
import type { Logger } from '../loggers/index';
import { retrier, type RetryOptions } from '../retry/index';
import { sleep } from '../utils/wait';
import { SHARE_ACKNOWLEDGE_TYPE, type ShareAcknowledgeType } from './acknowledge-types';
import { FETCH, FETCH_START } from './instrumentation-events';
import type { ShareBatch } from './share-batch';
import type { ShareGroup } from './share-group';

const FETCH_RETRY_BACKOFF_MS = 100;

/** Retriable on its own, or a retriable error the fetch retrier gave up on (`KafkaNumberOfRetriesExceeded`). */
function isRetriableFetchError(error: unknown): boolean {
  if (!(error instanceof KafkaError)) return false;
  return error.retriable || (error.cause instanceof KafkaError && error.cause.retriable);
}

export interface EachShareBatchPayload {
  batch: ShareBatch;
  heartbeat: () => Promise<void>;
  acknowledge: (type?: ShareAcknowledgeType) => void;
  isRunning: () => boolean;
  isStale: () => boolean;
}

export type EachShareBatchHandler = (payload: EachShareBatchPayload) => Promise<void>;

export interface ShareRunnerOptions {
  logger: Logger;
  shareGroup: ShareGroup;
  eachMessage?: EachMessageHandler | null;
  eachBatch?: EachShareBatchHandler | null;
  eachBatchAutoAck?: boolean;
  heartbeatInterval: number;
  concurrency?: number;
  prefetchMaxBatches?: number;
  prefetchMaxBytes?: number;
  onCrash: (reason: Error) => void | Promise<void>;
  retry?: RetryOptions;
  instrumentationEmitter?: InstrumentationEventEmitter | null;
}

export class ShareRunner {
  readonly #logger: Logger;
  readonly #shareGroup: ShareGroup;
  readonly #eachMessage: EachMessageHandler | null;
  readonly #eachBatch: EachShareBatchHandler | null;
  readonly #eachBatchAutoAck: boolean;
  readonly #heartbeatInterval: number;
  readonly #onCrash: (reason: Error) => void | Promise<void>;
  readonly #retrier: ReturnType<typeof retrier>;
  readonly #fetchManager: FetchManager<ShareBatch>;
  readonly #instrumentationEmitter: InstrumentationEventEmitter;

  running = false;
  shuttingDown = false;

  constructor({
    logger,
    shareGroup,
    eachMessage = null,
    eachBatch = null,
    eachBatchAutoAck = true,
    heartbeatInterval,
    concurrency = 1,
    prefetchMaxBatches,
    prefetchMaxBytes,
    onCrash,
    retry,
    instrumentationEmitter,
  }: ShareRunnerOptions) {
    this.#logger = logger.namespace('ShareRunner');
    this.#instrumentationEmitter = instrumentationEmitter ?? new InstrumentationEventEmitter();
    this.#shareGroup = shareGroup;
    this.#eachMessage = eachMessage;
    this.#eachBatch = eachBatch;
    this.#eachBatchAutoAck = eachBatchAutoAck;
    this.#heartbeatInterval = heartbeatInterval;
    this.#onCrash = onCrash;
    this.#retrier = retrier(retry);
    this.#fetchManager = createFetchManager<ShareBatch>({
      logger: this.#logger,
      getNodeIds: () => this.#shareGroup.getNodeIds(),
      fetch: (nodeId) => this.#fetchFromNode(nodeId),
      handler: (batch) => this.#handleBatch(batch),
      concurrency,
      prefetchMaxBatches,
      prefetchMaxBytes,
      isStale: (batch) => this.#isStale(batch),
    });
  }

  async start(): Promise<void> {
    if (this.running || this.shuttingDown) return;

    try {
      await this.#shareGroup.connect();
      if (this.shuttingDown) return;
      await this.#shareGroup.joinAndSync();
      if (this.shuttingDown) {
        await this.#shareGroup.leave();
        return;
      }
    } catch (error) {
      await this.#onCrash(error as Error);
      return;
    }

    this.running = true;
    void this.#heartbeatLoop().catch((error: unknown) => {
      this.running = false;
      this.#logger.error(`Unhandled error in heartbeat loop: ${(error as Error).message}`, {
        stack: (error as Error).stack,
      });
    });
    void this.#loop().catch((error: unknown) => {
      this.running = false;
      this.#logger.error(`Unhandled error in fetch loop: ${(error as Error).message}`, {
        stack: (error as Error).stack,
      });
    });
  }

  async stop(): Promise<void> {
    this.shuttingDown = true;
    try {
      await this.#fetchManager.stop();
    } catch {
      // Fetchers may already have exited.
    }
    while (this.running) {
      await sleep(50);
    }
    await this.#shareGroup.closeShareSessions();
    await this.#shareGroup.leave();
    await this.#shareGroup.disconnect();
  }

  async #heartbeatLoop(): Promise<void> {
    while (this.running && !this.shuttingDown) {
      try {
        await this.#shareGroup.heartbeat({ force: true });
      } catch (error) {
        if (this.shuttingDown) return;
        await this.#onCrash(error as Error);
        this.running = false;
        return;
      }
      const waitMs = this.#shareGroup.heartbeatIntervalMs ?? this.#heartbeatInterval;
      await sleep(Math.max(50, waitMs));
    }
  }

  async #loop(): Promise<void> {
    while (this.running && !this.shuttingDown) {
      try {
        if (this.#shareGroup.getNodeIds().length === 0) {
          await sleep(100);
          continue;
        }
        await this.#fetchManager.start();
      } catch (error) {
        if (this.shuttingDown || !this.running) break;
        if (error instanceof KafkaNoBrokerAvailableError) {
          await sleep(FETCH_RETRY_BACKOFF_MS);
          continue;
        }
        await this.#shareGroup.recoverFromFetch(error);
        if (this.shuttingDown || !this.running) break;
        if (isRetriableFetchError(error)) {
          await sleep(FETCH_RETRY_BACKOFF_MS);
          continue;
        }
        await this.#onCrash(error as Error);
        this.running = false;
        return;
      }
    }
    this.running = false;
  }

  #isStale(batch: ShareBatch): boolean {
    return !this.running || this.shuttingDown || !this.#shareGroup.hasAssignment(batch.topic, batch.partition);
  }

  async #fetchFromNode(nodeId: string): Promise<ShareBatch[]> {
    if (!this.running || this.shuttingDown) return [];

    const startFetch = Date.now();
    this.#instrumentationEmitter.emit(FETCH_START, { nodeId });

    const batches = (await this.#shareGroup.fetch(nodeId)).filter((batch) => !this.#isStale(batch));

    this.#instrumentationEmitter.emit(FETCH, {
      numberOfBatches: batches.length,
      duration: Date.now() - startFetch,
      nodeId,
    });

    return batches;
  }

  async #handleBatch(batch: ShareBatch): Promise<void> {
    if (this.#isStale(batch)) {
      this.#shareGroup.acknowledge(batch, SHARE_ACKNOWLEDGE_TYPE.RELEASE);
      return;
    }

    try {
      if (this.#eachBatch) {
        let acknowledged = false;
        await this.#eachBatch({
          batch,
          heartbeat: async () => {
            if (this.#shareGroup.heartbeatDue(this.#heartbeatInterval)) {
              await this.#shareGroup.heartbeat();
            }
          },
          acknowledge: (type = SHARE_ACKNOWLEDGE_TYPE.ACCEPT) => {
            acknowledged = true;
            this.#shareGroup.acknowledge(batch, type);
          },
          isRunning: () => this.running && !this.shuttingDown,
          isStale: () => this.#isStale(batch),
        });
        if (!acknowledged && this.#eachBatchAutoAck) {
          this.#shareGroup.acknowledge(batch, SHARE_ACKNOWLEDGE_TYPE.ACCEPT);
        }
        return;
      }

      const eachMessage = this.#eachMessage;
      if (!eachMessage) return;

      const { topic, partition } = batch;
      for (const message of batch.messages) {
        if (this.#isStale(batch)) break;
        await this.#retrier(() =>
          eachMessage({
            topic,
            partition,
            message,
            heartbeat: async () => {
              await this.#shareGroup.heartbeat({ force: true });
            },
            pause: () => () => {},
          }),
        );
      }
      this.#shareGroup.acknowledge(batch, SHARE_ACKNOWLEDGE_TYPE.ACCEPT);
    } catch (error) {
      this.#shareGroup.acknowledge(batch, SHARE_ACKNOWLEDGE_TYPE.RELEASE);
      this.running = false;
      await this.#onCrash(error as Error);
    }
  }
}
