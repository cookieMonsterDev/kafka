import { KafkaError } from '../errors';
import type { ShareFetchAcknowledgementBatchInput } from '../protocol/requests/share-fetch/index';
import { retrier, type RetryOptions } from '../retry/index';
import { sleep } from '../utils/wait';
import type { EachMessageHandler } from '../consumer/types';
import { SHARE_ACKNOWLEDGE_TYPE } from './acknowledge-types';
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
  readonly #onCrash: (reason: Error) => void | Promise<void>;

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
    this.#onCrash = onCrash;
    retrier(retry);
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
    void this.#loop();
  }

  async stop(): Promise<void> {
    this.shuttingDown = true;
    while (this.running) {
      await sleep(50);
    }
    await this.#shareGroup.leave();
    await this.#shareGroup.disconnect();
  }

  async #loop(): Promise<void> {
    while (this.running && !this.shuttingDown) {
      try {
        await this.#shareGroup.heartbeat({ interval: this.#heartbeatInterval });
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

    const shareSessionEpoch = this.#nextShareSessionEpoch(nodeId);
    const topics = topicPartitions
      .map(({ topic, partitions }) => {
        const topicId = this.#shareGroup.cluster.findTopicId(topic);
        if (!topicId) return null;
        return {
          topicId,
          partitions: partitions.map((partitionIndex) => ({
            partitionIndex,
            acknowledgementBatches: this.#acknowledgementBatchesFor(topic, partitionIndex),
          })),
        };
      })
      .filter((entry): entry is NonNullable<typeof entry> => entry != null);

    if (topics.length === 0) return [];

    const broker = await this.#shareGroup.cluster.findBroker({ nodeId });
    const response = await broker.shareFetch({
      groupId: this.#shareGroup.groupId,
      memberId,
      shareSessionEpoch,
      maxWaitMs: this.#maxWaitTimeInMs,
      minBytes: this.#minBytes,
      maxBytes: this.#maxBytes,
      maxRecords: this.#maxRecords,
      batchSize: this.#batchSize,
      topics,
      forgottenTopics: [],
    });

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
  }

  async #resolveTopicName(topicId: Buffer): Promise<string> {
    for (const { topic } of this.#shareGroup.assigned()) {
      const id = this.#shareGroup.cluster.findTopicId(topic);
      if (id?.equals(topicId)) return topic;
    }
    throw new KafkaError('Unknown topic id in share fetch response');
  }

  #acknowledgementBatchesFor(topic: string, partition: number): ShareFetchAcknowledgementBatchInput[] {
    return this.#pendingAcks
      .filter((ack) => ack.topic === topic && ack.partition === partition)
      .map(({ firstOffset, lastOffset }) => ({
        firstOffset,
        lastOffset,
        acknowledgeTypes: [SHARE_ACKNOWLEDGE_TYPE.ACCEPT],
      }));
  }

  #nextShareSessionEpoch(nodeId: string): number {
    const current = this.#shareSessionEpochByNode.get(nodeId) ?? -1;
    const next = current < 0 ? 0 : current + 1;
    this.#shareSessionEpochByNode.set(nodeId, next);
    return next;
  }

  async #handleBatch(batch: ShareBatch): Promise<void> {
    if (batch.isEmpty()) return;

    const { topic, partition } = batch;
    for (const message of batch.messages) {
      await this.#eachMessage({
        topic,
        partition,
        message,
        heartbeat: async () => {
          await this.#shareGroup.heartbeat({ force: true });
        },
        pause: () => () => {},
      });
    }

    for (const acquired of batch.acquiredRecords) {
      this.#pendingAcks.push({
        topic,
        partition,
        firstOffset: acquired.firstOffset,
        lastOffset: acquired.lastOffset,
      });
    }
  }
}
