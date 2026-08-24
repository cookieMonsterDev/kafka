import { createFetchManager, type FetchManager } from '../consumer/fetch-manager';
import type { EachMessageHandler } from '../consumer/types';
import { KafkaError, KafkaNoBrokerAvailableError } from '../errors';
import type { Logger } from '../loggers/index';
import {
  SHARE_ACQUIRE_MODE,
  SHARE_SESSION_CLOSE_EPOCH,
  SHARE_SESSION_INITIAL_EPOCH,
  type ShareAcquireMode,
  type ShareFetchAcknowledgementBatchInput,
  type ShareFetchForgottenTopicInput,
} from '../protocol/requests/share-fetch/index';
import { retrier, type RetryOptions } from '../retry/index';
import { sleep } from '../utils/wait';
import { SHARE_ACKNOWLEDGE_TYPE, type ShareAcknowledgeType } from './acknowledge-types';
import { ShareBatch } from './share-batch';
import type { ShareGroup } from './share-group';

const DEFAULT_MAX_WAIT_MS = 5000;
const DEFAULT_MIN_BYTES = 1;
const DEFAULT_MAX_BYTES = 50 * 1024 * 1024;
const DEFAULT_MAX_RECORDS = 500;
const DEFAULT_BATCH_SIZE = 100;

interface PendingAck {
  topic: string;
  partition: number;
  firstOffset: bigint;
  lastOffset: bigint;
  acknowledgeType: ShareAcknowledgeType;
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
  maxWaitTimeInMs?: number;
  minBytes?: number;
  maxBytes?: number;
  maxRecords?: number;
  batchSize?: number;
  shareAcquireMode?: ShareAcquireMode;
  concurrency?: number;
  prefetchMaxBatches?: number;
  prefetchMaxBytes?: number;
  onCrash: (reason: Error) => void | Promise<void>;
  retry?: RetryOptions;
}

export class ShareRunner {
  readonly #logger: Logger;
  readonly #shareGroup: ShareGroup;
  readonly #eachMessage: EachMessageHandler | null;
  readonly #eachBatch: EachShareBatchHandler | null;
  readonly #eachBatchAutoAck: boolean;
  readonly #heartbeatInterval: number;
  readonly #maxWaitTimeInMs: number;
  readonly #minBytes: number;
  readonly #maxBytes: number;
  readonly #maxRecords: number;
  readonly #batchSize: number;
  readonly #shareAcquireMode: ShareAcquireMode;
  readonly #onCrash: (reason: Error) => void | Promise<void>;
  readonly #retrier: ReturnType<typeof retrier>;
  readonly #fetchManager: FetchManager<ShareBatch>;

  running = false;
  shuttingDown = false;
  #shareSessionEpochByNode = new Map<string, number>();
  /** What each node's share session last had, so unassigned partitions can be forgotten (KIP-227-style). */
  #shareSessionPartitionsByNode = new Map<string, Map<string, Set<number>>>();
  #pendingAcks: PendingAck[] = [];

  constructor({
    logger,
    shareGroup,
    eachMessage = null,
    eachBatch = null,
    eachBatchAutoAck = true,
    heartbeatInterval,
    maxWaitTimeInMs = DEFAULT_MAX_WAIT_MS,
    minBytes = DEFAULT_MIN_BYTES,
    maxBytes = DEFAULT_MAX_BYTES,
    maxRecords = DEFAULT_MAX_RECORDS,
    batchSize = DEFAULT_BATCH_SIZE,
    shareAcquireMode = SHARE_ACQUIRE_MODE.BATCH_OPTIMIZED,
    concurrency = 1,
    prefetchMaxBatches,
    prefetchMaxBytes,
    onCrash,
    retry,
  }: ShareRunnerOptions) {
    this.#logger = logger.namespace('ShareRunner');
    this.#shareGroup = shareGroup;
    this.#eachMessage = eachMessage;
    this.#eachBatch = eachBatch;
    this.#eachBatchAutoAck = eachBatchAutoAck;
    this.#heartbeatInterval = heartbeatInterval;
    this.#maxWaitTimeInMs = maxWaitTimeInMs;
    this.#minBytes = minBytes;
    this.#maxBytes = maxBytes;
    this.#maxRecords = maxRecords;
    this.#batchSize = batchSize;
    this.#shareAcquireMode = shareAcquireMode;
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
    void this.#heartbeatLoop();
    void this.#loop();
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
    await this.#flushAndCloseSessions();
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
          await sleep(100);
          continue;
        }
        await this.#shareGroup.recoverFromFetch(error);
        if (this.shuttingDown || !this.running) break;
        if (error instanceof KafkaError) continue;
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

    await this.#shareGroup.cluster.refreshMetadataIfNecessary();
    const topicPartitions = this.#shareGroup.filterPartitionsByNode(nodeId, this.#shareGroup.assigned());
    if (topicPartitions.length === 0) return [];

    const memberId = this.#shareGroup.memberId;
    if (!memberId) return [];

    const shareSessionEpoch = this.#shareSessionEpochByNode.get(nodeId) ?? SHARE_SESSION_INITIAL_EPOCH;
    const previousPartitions = this.#shareSessionPartitionsByNode.get(nodeId);
    const currentPartitions = new Map(topicPartitions.map(({ topic, partitions }) => [topic, new Set(partitions)]));
    const forgottenTopics = this.#forgottenTopicsFor(previousPartitions, currentPartitions);

    const takenAcks: PendingAck[] = [];
    const topics = topicPartitions
      .map(({ topic, partitions }) => {
        const topicId = this.#shareGroup.cluster.findTopicId(topic);
        if (!topicId) return null;
        return {
          topicId,
          partitions: partitions.map((partitionIndex) => {
            const acknowledgementBatches = this.#takeAcknowledgementBatchesFor(topic, partitionIndex, takenAcks);
            return { partitionIndex, acknowledgementBatches };
          }),
        };
      })
      .filter((entry): entry is NonNullable<typeof entry> => entry != null);

    if (topics.length === 0) {
      this.#pendingAcks.push(...takenAcks);
      return [];
    }

    try {
      const broker = await this.#shareGroup.cluster.findBroker({ nodeId });
      const response = await this.#retrier(() =>
        broker.shareFetch({
          groupId: this.#shareGroup.groupId,
          memberId,
          shareSessionEpoch,
          maxWaitMs: this.#maxWaitTimeInMs,
          minBytes: this.#minBytes,
          maxBytes: this.#maxBytes,
          maxRecords: this.#maxRecords,
          batchSize: this.#batchSize,
          shareAcquireMode: this.#shareAcquireMode,
          isRenewAck: takenAcks.some((ack) => ack.acknowledgeType === SHARE_ACKNOWLEDGE_TYPE.RENEW),
          topics,
          forgottenTopics,
        }),
      );
      this.#shareSessionEpochByNode.set(nodeId, shareSessionEpoch + 1);
      this.#shareSessionPartitionsByNode.set(nodeId, currentPartitions);

      const batches: ShareBatch[] = [];
      for (const { topicId, partitions } of response.responses) {
        const topic = await this.#resolveTopicName(topicId);
        for (const partitionData of partitions) {
          batches.push(
            new ShareBatch(topic, {
              partition: partitionData.partitionIndex,
              messages: partitionData.records,
              acquiredRecords: partitionData.acquiredRecords,
            }),
          );
        }
      }
      return batches.filter((batch) => !this.#isStale(batch));
    } catch (error) {
      this.#pendingAcks.push(...takenAcks);
      // The session state we just tried to send may no longer match what the broker has -
      // start the next fetch for this node with a fresh (epoch 0) session. `#retrier` wraps an
      // exhausted-retries error in `KafkaNumberOfRetriesExceeded`, so the protocol error's `type`
      // may only be reachable via `.cause`.
      const errorLike = error as { type?: string; cause?: { type?: string } };
      const type = errorLike.type ?? errorLike.cause?.type;
      if (type === 'SHARE_SESSION_NOT_FOUND' || type === 'INVALID_SHARE_SESSION_EPOCH') {
        this.#shareSessionEpochByNode.delete(nodeId);
        this.#shareSessionPartitionsByNode.delete(nodeId);
      }
      throw error;
    }
  }

  /** Partitions the node's share session previously held but are no longer assigned must be forgotten. */
  #forgottenTopicsFor(
    previous: Map<string, Set<number>> | undefined,
    current: Map<string, Set<number>>,
  ): ShareFetchForgottenTopicInput[] {
    if (!previous) return [];

    const forgottenTopics: ShareFetchForgottenTopicInput[] = [];
    for (const [topic, previousPartitions] of previous) {
      const currentTopicPartitions = current.get(topic);
      const removed = [...previousPartitions].filter((partition) => !currentTopicPartitions?.has(partition));
      if (removed.length === 0) continue;

      const topicId = this.#shareGroup.cluster.findTopicId(topic);
      if (!topicId) continue;
      forgottenTopics.push({ topicId, partitions: removed });
    }
    return forgottenTopics;
  }

  async #resolveTopicName(topicId: Buffer): Promise<string> {
    for (const { topic } of this.#shareGroup.assigned()) {
      const id = this.#shareGroup.cluster.findTopicId(topic);
      if (id?.equals(topicId)) return topic;
    }
    throw new KafkaError('Unknown topic id in share fetch response');
  }

  #takeAcknowledgementBatchesFor(
    topic: string,
    partition: number,
    taken: PendingAck[],
  ): ShareFetchAcknowledgementBatchInput[] {
    const remaining: PendingAck[] = [];
    const batches: ShareFetchAcknowledgementBatchInput[] = [];
    for (const ack of this.#pendingAcks) {
      if (ack.topic === topic && ack.partition === partition) {
        taken.push(ack);
        batches.push({
          firstOffset: ack.firstOffset,
          lastOffset: ack.lastOffset,
          acknowledgeTypes: [ack.acknowledgeType],
        });
      } else {
        remaining.push(ack);
      }
    }
    this.#pendingAcks = remaining;
    return batches;
  }

  #queueAcks(batch: ShareBatch, acknowledgeType: ShareAcknowledgeType): void {
    for (const acquired of batch.acquiredRecords) {
      this.#pendingAcks.push({
        topic: batch.topic,
        partition: batch.partition,
        firstOffset: acquired.firstOffset,
        lastOffset: acquired.lastOffset,
        acknowledgeType,
      });
    }
  }

  async #handleBatch(batch: ShareBatch): Promise<void> {
    if (this.#isStale(batch)) {
      this.#queueAcks(batch, SHARE_ACKNOWLEDGE_TYPE.RELEASE);
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
            this.#queueAcks(batch, type);
          },
          isRunning: () => this.running && !this.shuttingDown,
          isStale: () => this.#isStale(batch),
        });
        if (!acknowledged && this.#eachBatchAutoAck) {
          this.#queueAcks(batch, SHARE_ACKNOWLEDGE_TYPE.ACCEPT);
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
      this.#queueAcks(batch, SHARE_ACKNOWLEDGE_TYPE.ACCEPT);
    } catch (error) {
      this.#queueAcks(batch, SHARE_ACKNOWLEDGE_TYPE.RELEASE);
      this.running = false;
      await this.#onCrash(error as Error);
    }
  }

  async #flushAndCloseSessions(): Promise<void> {
    const memberId = this.#shareGroup.memberId;
    if (!memberId) {
      this.#pendingAcks = [];
      this.#shareSessionEpochByNode.clear();
      this.#shareSessionPartitionsByNode.clear();
      return;
    }

    const nodeIds = new Set([...this.#shareSessionEpochByNode.keys(), ...this.#shareGroup.getNodeIds()]);
    for (const nodeId of nodeIds) {
      try {
        const broker = await this.#shareGroup.cluster.findBroker({ nodeId });
        await broker.shareAcknowledge({
          groupId: this.#shareGroup.groupId,
          memberId,
          shareSessionEpoch: SHARE_SESSION_CLOSE_EPOCH,
          isRenewAck: this.#pendingAcks.some((ack) => ack.acknowledgeType === SHARE_ACKNOWLEDGE_TYPE.RENEW),
          topics: this.#acknowledgementTopicsForNode(nodeId),
        });
      } catch {
        // Closing is best-effort; leave() still runs after this.
      }
    }

    this.#pendingAcks = [];
    this.#shareSessionEpochByNode.clear();
    this.#shareSessionPartitionsByNode.clear();
  }

  #acknowledgementTopicsForNode(nodeId: string): {
    topicId: Buffer;
    partitions: {
      partitionIndex: number;
      acknowledgementBatches: ShareFetchAcknowledgementBatchInput[];
    }[];
  }[] {
    const topicPartitions = this.#shareGroup.filterPartitionsByNode(nodeId, this.#shareGroup.assigned());
    const partitionSet = new Set(
      topicPartitions.flatMap(({ topic, partitions }) => partitions.map((partition) => `${topic}:${partition}`)),
    );

    const byTopic = new Map<
      string,
      { topicId: Buffer; partitions: Map<number, ShareFetchAcknowledgementBatchInput[]> }
    >();

    for (const ack of this.#pendingAcks) {
      if (!partitionSet.has(`${ack.topic}:${ack.partition}`)) continue;
      const topicId = this.#shareGroup.cluster.findTopicId(ack.topic);
      if (!topicId) continue;
      const existing = byTopic.get(ack.topic) ?? {
        topicId,
        partitions: new Map<number, ShareFetchAcknowledgementBatchInput[]>(),
      };
      const batches = existing.partitions.get(ack.partition) ?? [];
      batches.push({
        firstOffset: ack.firstOffset,
        lastOffset: ack.lastOffset,
        acknowledgeTypes: [ack.acknowledgeType],
      });
      existing.partitions.set(ack.partition, batches);
      byTopic.set(ack.topic, existing);
    }

    return [...byTopic.values()].map(({ topicId, partitions }) => ({
      topicId,
      partitions: [...partitions.entries()].map(([partitionIndex, acknowledgementBatches]) => ({
        partitionIndex,
        acknowledgementBatches,
      })),
    }));
  }
}
