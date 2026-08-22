import { KafkaError } from '../errors';
import {
  SHARE_ACQUIRE_MODE,
  SHARE_SESSION_CLOSE_EPOCH,
  SHARE_SESSION_INITIAL_EPOCH,
  type ShareAcquireMode,
  type ShareFetchAcknowledgementBatchInput,
} from '../protocol/requests/share-fetch/index';
import { retrier, type RetryOptions } from '../retry/index';
import { sleep } from '../utils/wait';
import type { EachMessageHandler } from '../consumer/types';
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

export interface ShareRunnerOptions {
  shareGroup: ShareGroup;
  eachMessage: EachMessageHandler;
  heartbeatInterval: number;
  maxWaitTimeInMs?: number;
  minBytes?: number;
  maxBytes?: number;
  maxRecords?: number;
  batchSize?: number;
  shareAcquireMode?: ShareAcquireMode;
  onCrash: (reason: Error) => void | Promise<void>;
  retry?: RetryOptions;
}

export class ShareRunner {
  readonly #shareGroup: ShareGroup;
  readonly #eachMessage: EachMessageHandler;
  readonly #heartbeatInterval: number;
  readonly #maxWaitTimeInMs: number;
  readonly #minBytes: number;
  readonly #maxBytes: number;
  readonly #maxRecords: number;
  readonly #batchSize: number;
  readonly #shareAcquireMode: ShareAcquireMode;
  readonly #onCrash: (reason: Error) => void | Promise<void>;
  readonly #retrier: ReturnType<typeof retrier>;

  running = false;
  shuttingDown = false;
  #shareSessionEpochByNode = new Map<string, number>();
  #pendingAcks: PendingAck[] = [];

  constructor({
    shareGroup,
    eachMessage,
    heartbeatInterval,
    maxWaitTimeInMs = DEFAULT_MAX_WAIT_MS,
    minBytes = DEFAULT_MIN_BYTES,
    maxBytes = DEFAULT_MAX_BYTES,
    maxRecords = DEFAULT_MAX_RECORDS,
    batchSize = DEFAULT_BATCH_SIZE,
    shareAcquireMode = SHARE_ACQUIRE_MODE.BATCH_OPTIMIZED,
    onCrash,
    retry,
  }: ShareRunnerOptions) {
    this.#shareGroup = shareGroup;
    this.#eachMessage = eachMessage;
    this.#heartbeatInterval = heartbeatInterval;
    this.#maxWaitTimeInMs = maxWaitTimeInMs;
    this.#minBytes = minBytes;
    this.#maxBytes = maxBytes;
    this.#maxRecords = maxRecords;
    this.#batchSize = batchSize;
    this.#shareAcquireMode = shareAcquireMode;
    this.#onCrash = onCrash;
    this.#retrier = retrier(retry);
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
        const nodeIds = this.#shareGroup.getNodeIds();
        for (const nodeId of nodeIds) {
          const batches = await this.#fetchFromNode(nodeId);
          for (const batch of batches) {
            await this.#handleBatch(batch);
          }
        }
        if (nodeIds.length === 0) await sleep(this.#maxWaitTimeInMs);
      } catch (error) {
        await this.#shareGroup.recoverFromFetch(error);
        if (error instanceof KafkaError) continue;
        await this.#onCrash(error as Error);
        this.running = false;
        return;
      }
    }
    this.running = false;
  }

  async #fetchFromNode(nodeId: string): Promise<ShareBatch[]> {
    await this.#shareGroup.cluster.refreshMetadataIfNecessary();
    const topicPartitions = this.#shareGroup.filterPartitionsByNode(nodeId, this.#shareGroup.assigned());
    if (topicPartitions.length === 0) return [];

    const memberId = this.#shareGroup.memberId;
    if (!memberId) return [];

    const shareSessionEpoch = this.#shareSessionEpochByNode.get(nodeId) ?? SHARE_SESSION_INITIAL_EPOCH;
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
          forgottenTopics: [],
        }),
      );
      this.#shareSessionEpochByNode.set(nodeId, shareSessionEpoch + 1);

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
      return batches;
    } catch (error) {
      this.#pendingAcks.push(...takenAcks);
      throw error;
    }
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
    try {
      const { topic, partition } = batch;
      for (const message of batch.messages) {
        await this.#retrier(() =>
          this.#eachMessage({
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
