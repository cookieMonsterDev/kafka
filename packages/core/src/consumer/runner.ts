import { EventEmitter } from 'node:events';
import { isKafkaError, isRebalancing } from '../errors';
import type { InstrumentationEventEmitter } from '../instrumentation/emitter';
import type { Logger } from '../loggers/index';
import { retrier, type RetryOptions } from '../retry/index';
import type { Batch } from './batch';
import type { ConsumerGroupHandle } from './consumer-group';
import { createFetchManager, type FetchManager } from './fetch-manager';
import { END_BATCH_PROCESS, FETCH, FETCH_START, REBALANCING, START_BATCH_PROCESS } from './instrumentation-events';
import type { EachBatchHandler, EachMessageHandler, Offsets } from './types';

const CONSUMING_START = 'consuming-start';
const CONSUMING_STOP = 'consuming-stop';

export interface RunnerOptions {
  logger: Logger;
  consumerGroup: ConsumerGroupHandle;
  instrumentationEmitter: InstrumentationEventEmitter;
  eachBatchAutoResolve?: boolean;
  concurrency: number;
  eachBatch?: EachBatchHandler | null;
  eachMessage?: EachMessageHandler | null;
  heartbeatInterval: number;
  onCrash: (reason: Error) => void | Promise<void>;
  retry?: RetryOptions;
  autoCommit?: boolean;
}

export class Runner extends EventEmitter {
  logger: Logger;
  consumerGroup: ConsumerGroupHandle;
  instrumentationEmitter: InstrumentationEventEmitter;
  eachBatchAutoResolve: boolean;
  eachBatch: EachBatchHandler | null | undefined;
  eachMessage: EachMessageHandler | null | undefined;
  heartbeatInterval: number;
  retrier: ReturnType<typeof retrier>;
  onCrash: (reason: Error) => void | Promise<void>;
  autoCommit: boolean;
  fetchManager: FetchManager<Batch>;
  running = false;
  shuttingDown = false;
  #consuming = false;
  #starting = false;
  #stopPromise: Promise<void> | null = null;

  constructor({
    logger,
    consumerGroup,
    instrumentationEmitter,
    eachBatchAutoResolve = true,
    concurrency,
    eachBatch,
    eachMessage,
    heartbeatInterval,
    onCrash,
    retry,
    autoCommit = true,
  }: RunnerOptions) {
    super();
    this.logger = logger.namespace('Runner');
    this.consumerGroup = consumerGroup;
    this.instrumentationEmitter = instrumentationEmitter;
    this.eachBatchAutoResolve = eachBatchAutoResolve;
    this.eachBatch = eachBatch;
    this.eachMessage = eachMessage;
    this.heartbeatInterval = heartbeatInterval;
    this.retrier = retrier({ ...retry });
    this.onCrash = onCrash;
    this.autoCommit = autoCommit;
    this.fetchManager = createFetchManager({
      logger: this.logger,
      getNodeIds: () => this.consumerGroup.getNodeIds(),
      fetch: (nodeId) => this.fetch(nodeId),
      handler: (batch) => this.handleBatch(batch),
      concurrency,
    });
  }

  get consuming(): boolean {
    return this.#consuming;
  }

  set consuming(value: boolean) {
    if (this.#consuming !== value) {
      this.#consuming = value;
      this.emit(value ? CONSUMING_START : CONSUMING_STOP);
    }
  }

  async start(): Promise<void> {
    if (this.running || this.shuttingDown) return;

    this.#starting = true;
    try {
      await this.consumerGroup.connect();
      if (this.shuttingDown) return;
      await this.consumerGroup.joinAndSync();
      if (this.shuttingDown) {
        await this.consumerGroup.leave();
        return;
      }
    } catch (e) {
      await this.onCrash(e as Error);
      return;
    } finally {
      this.#starting = false;
    }

    this.running = true;
    this.scheduleFetchManager();
  }

  scheduleFetchManager = (): void => {
    if (!this.running || this.shuttingDown) {
      this.consuming = false;
      this.logger.info('consumer not running, exiting', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
      });
      return;
    }

    this.consuming = true;

    this.retrier(async (bail, retryCount, retryTime) => {
      if (!this.running || this.shuttingDown) return;

      try {
        await this.fetchManager.start();
      } catch (e) {
        const error = e as Error & { type?: string };

        if (isRebalancing(error)) {
          if (!this.running || this.shuttingDown) return;

          this.logger.warn('The group is rebalancing, re-joining', {
            groupId: this.consumerGroup.groupId,
            memberId: this.consumerGroup.memberId,
            error: error.message,
          });

          this.instrumentationEmitter.emit(REBALANCING, {
            groupId: this.consumerGroup.groupId,
            memberId: this.consumerGroup.memberId,
          });

          await this.consumerGroup.joinAndSync();
          return;
        }

        if (error.type === 'UNKNOWN_MEMBER_ID') {
          if (!this.running || this.shuttingDown) return;

          this.logger.error('The coordinator is not aware of this member, re-joining the group', {
            groupId: this.consumerGroup.groupId,
            memberId: this.consumerGroup.memberId,
            error: error.message,
          });

          this.consumerGroup.memberId = null;
          await this.consumerGroup.joinAndSync();
          return;
        }

        if (error.name === 'KafkaNotImplemented' || error.name === 'KafkaNoBrokerAvailableError') {
          bail(error);
          return;
        }

        this.logger.debug('Error while scheduling fetch manager, trying again...', {
          groupId: this.consumerGroup.groupId,
          memberId: this.consumerGroup.memberId,
          error: error.message,
          stack: error.stack,
          retryCount,
          retryTime,
        });

        throw error;
      }
    })
      .then(() => {
        this.scheduleFetchManager();
      })
      .catch((e: Error) => {
        void this.onCrash(e);
        this.consuming = false;
        this.running = false;
      });
  };

  async stop(): Promise<void> {
    this.#stopPromise ??= this.#performStop();
    return this.#stopPromise;
  }

  async #performStop(): Promise<void> {
    this.shuttingDown = true;
    this.consumerGroup.shuttingDown = true;

    const shouldCleanup = this.running || this.#starting || this.#consuming;
    this.running = false;

    if (!shouldCleanup) return;

    this.logger.debug('stop consumer group', {
      groupId: this.consumerGroup.groupId,
      memberId: this.consumerGroup.memberId,
    });

    try {
      await this.fetchManager.stop();
      await this.waitForConsumer();
      await this.consumerGroup.leave();
    } catch {
      // Swallow stop errors, matching the original shutdown behavior.
    }
  }

  waitForConsumer(): Promise<void> {
    return new Promise((resolve) => {
      if (!this.consuming) {
        resolve();
        return;
      }

      this.logger.debug('waiting for consumer to finish...', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
      });

      const timeoutId = setTimeout(resolve, 10_000);
      this.once(CONSUMING_STOP, () => {
        clearTimeout(timeoutId);
        resolve();
      });
    });
  }

  async heartbeat(): Promise<void> {
    try {
      await this.consumerGroup.heartbeat({ interval: this.heartbeatInterval });
    } catch (e) {
      if (isRebalancing(e as { type?: string })) {
        await this.autoCommitOffsets();
      }
      throw e;
    }
  }

  async processEachMessage(batch: Batch): Promise<void> {
    const { topic, partition } = batch;
    const eachMessage = this.eachMessage;
    if (!eachMessage) return;

    const pause = (): (() => void) => {
      this.consumerGroup.pause([{ topic, partitions: [partition] }]);
      return () => this.consumerGroup.resume([{ topic, partitions: [partition] }]);
    };

    for (const message of batch.messages) {
      if (!this.running || this.shuttingDown || this.consumerGroup.hasSeekOffset({ topic, partition })) {
        break;
      }

      try {
        await eachMessage({
          topic,
          partition,
          message,
          heartbeat: () => this.heartbeat(),
          pause,
        });
      } catch (e) {
        if (!isKafkaError(e)) {
          this.logger.error(`Error when calling eachMessage`, {
            topic,
            partition,
            offset: message.offset,
            stack: (e as Error).stack,
            error: e,
          });
        }

        await this.autoCommitOffsets();
        throw e;
      }

      this.consumerGroup.resolveOffset({ topic, partition, offset: message.offset });
      await this.heartbeat();
      await this.autoCommitOffsetsIfNecessary();

      if (this.consumerGroup.isPaused(topic, partition)) {
        break;
      }
    }
  }

  async processEachBatch(batch: Batch): Promise<void> {
    const { topic, partition } = batch;
    const eachBatch = this.eachBatch;
    if (!eachBatch) return;

    const lastFilteredMessage = batch.messages[batch.messages.length - 1];

    const pause = (): (() => void) => {
      this.consumerGroup.pause([{ topic, partitions: [partition] }]);
      return () => this.consumerGroup.resume([{ topic, partitions: [partition] }]);
    };

    try {
      await eachBatch({
        batch,
        resolveOffset: (offset) => {
          /**
           * The transactional producer writes a control record as the last record of a
           * RecordBatch. That record is filtered before it reaches `eachBatch`. When
           * auto-resolve is disabled, user code can never resolve the control-record
           * offset, and the consumer would stall.
           *
           * Resolving the last filtered message offset therefore also resolves the batch's
           * last offset (the control record). See
           * https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/consumer/internals/Fetcher.java
           */
          const offsetToResolve =
            lastFilteredMessage && offset === lastFilteredMessage.offset ? batch.lastOffset() : offset;
          this.consumerGroup.resolveOffset({ topic, partition, offset: offsetToResolve });
        },
        heartbeat: () => this.heartbeat(),
        pause,
        commitOffsetsIfNecessary: async (offsets) => {
          return offsets ? this.consumerGroup.commitOffsets(offsets) : this.consumerGroup.commitOffsetsIfNecessary();
        },
        uncommittedOffsets: () => this.consumerGroup.uncommittedOffsets(),
        isRunning: () => this.running && !this.shuttingDown,
        isStale: () => this.consumerGroup.hasSeekOffset({ topic, partition }),
      });
    } catch (e) {
      if (!isKafkaError(e)) {
        this.logger.error(`Error when calling eachBatch`, {
          topic,
          partition,
          offset: batch.firstOffset(),
          stack: (e as Error).stack,
          error: e,
        });
      }

      await this.autoCommitOffsets();
      throw e;
    }

    if (this.eachBatchAutoResolve) {
      this.consumerGroup.resolveOffset({ topic, partition, offset: batch.lastOffset() });
    }
  }

  async fetch(nodeId: string): Promise<Batch[]> {
    if (!this.running || this.shuttingDown) {
      this.logger.debug('consumer not running, exiting', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
      });
      return [];
    }

    const startFetch = Date.now();
    this.instrumentationEmitter.emit(FETCH_START, { nodeId });

    const batches = await this.consumerGroup.fetch(nodeId);

    this.instrumentationEmitter.emit(FETCH, {
      // Always 0: fetchers deliver batches asynchronously, so a count isn't available here.
      // Kept for compatibility with existing instrumentation listeners.
      numberOfBatches: 0,
      duration: Date.now() - startFetch,
      nodeId,
    });

    if (batches.length === 0) {
      await this.heartbeat();
    }

    return batches;
  }

  async handleBatch(batch: Batch): Promise<void> {
    if (!this.running || this.shuttingDown) {
      this.logger.debug('consumer not running, exiting', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
      });
      return;
    }

    const startBatchProcess = Date.now();
    const payload = {
      topic: batch.topic,
      partition: batch.partition,
      highWatermark: batch.highWatermark,
      offsetLag: batch.offsetLag(),
      offsetLagLow: batch.offsetLagLow(),
      batchSize: batch.messages.length,
      firstOffset: batch.firstOffset(),
      lastOffset: batch.lastOffset(),
    };

    if (batch.isEmptyDueToFiltering()) {
      this.instrumentationEmitter.emit(START_BATCH_PROCESS, payload);
      this.consumerGroup.resolveOffset({
        topic: batch.topic,
        partition: batch.partition,
        offset: batch.lastOffset(),
      });
      await this.autoCommitOffsetsIfNecessary();
      this.instrumentationEmitter.emit(END_BATCH_PROCESS, {
        ...payload,
        duration: Date.now() - startBatchProcess,
      });
      await this.heartbeat();
      return;
    }

    if (batch.isEmpty()) {
      await this.heartbeat();
      return;
    }

    this.instrumentationEmitter.emit(START_BATCH_PROCESS, payload);

    if (this.eachMessage) {
      await this.processEachMessage(batch);
    } else if (this.eachBatch) {
      await this.processEachBatch(batch);
    }

    this.instrumentationEmitter.emit(END_BATCH_PROCESS, {
      ...payload,
      duration: Date.now() - startBatchProcess,
    });

    await this.autoCommitOffsets();
    await this.heartbeat();
  }

  autoCommitOffsets(): Promise<void> | undefined {
    if (this.autoCommit) {
      return this.consumerGroup.commitOffsets();
    }
    return undefined;
  }

  autoCommitOffsetsIfNecessary(): Promise<void> | undefined {
    if (this.autoCommit) {
      return this.consumerGroup.commitOffsetsIfNecessary();
    }
    return undefined;
  }

  commitOffsets(offsets: Offsets): Promise<void> | undefined {
    if (!this.running || this.shuttingDown) {
      this.logger.debug('consumer not running, exiting', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
        offsets,
      });
      return undefined;
    }

    return this.retrier(async (bail, retryCount, retryTime) => {
      try {
        await this.consumerGroup.commitOffsets(offsets);
      } catch (e) {
        const error = e as Error;

        if (!this.running || this.shuttingDown) {
          this.logger.debug('consumer not running, exiting', {
            error: error.message,
            groupId: this.consumerGroup.groupId,
            memberId: this.consumerGroup.memberId,
            offsets,
          });
          return;
        }

        if (error.name === 'KafkaNotImplemented') {
          bail(error);
          return;
        }

        this.logger.debug('Error while committing offsets, trying again...', {
          groupId: this.consumerGroup.groupId,
          memberId: this.consumerGroup.memberId,
          error: error.message,
          stack: error.stack,
          retryCount,
          retryTime,
          offsets,
        });

        throw error;
      }
    });
  }
}
