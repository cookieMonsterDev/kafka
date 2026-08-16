import { filterAbortedMessages, type AbortedTransaction } from './filter-aborted-messages.js';
import type { KafkaMessage } from './types.js';

export interface BatchPartitionData {
  partition: number;
  highWatermark: bigint;
  messages: readonly KafkaMessage[];
  abortedTransactions?: readonly AbortedTransaction[] | null;
}

/**
 * Messages from a single fetch response partition. A batch can contain multiple RecordBatches;
 * compressed fetches can also include offsets below the requested `fetchedOffset`, which are
 * discarded here (https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/consumer/internals/Fetcher.java).
 */
export class Batch {
  readonly topic: string;
  readonly partition: number;
  readonly highWatermark: bigint;
  readonly fetchedOffset: bigint;
  readonly rawMessages: readonly KafkaMessage[];
  readonly messagesWithinOffset: KafkaMessage[];
  readonly messages: KafkaMessage[];

  constructor(topic: string, fetchedOffset: bigint, partitionData: BatchPartitionData) {
    this.fetchedOffset = fetchedOffset;
    this.topic = topic;
    this.partition = partitionData.partition;
    this.highWatermark = partitionData.highWatermark;
    this.rawMessages = partitionData.messages;

    this.messagesWithinOffset = this.rawMessages.filter((message) => message.offset >= this.fetchedOffset);
    this.messages = filterAbortedMessages({
      messages: this.messagesWithinOffset,
      abortedTransactions: partitionData.abortedTransactions,
    }).filter((message) => !message.isControlRecord);
  }

  isEmpty(): boolean {
    return this.messages.length === 0;
  }

  isEmptyIncludingFiltered(): boolean {
    return this.messagesWithinOffset.length === 0;
  }

  /**
   * True when the broker returned records but every one was filtered out (control records,
   * aborted transactions, or log-compacted offsets below `fetchedOffset`). The consumer still
   * needs to resolve `lastOffset()` so it can move past the batch.
   */
  isEmptyDueToFiltering(): boolean {
    return this.isEmpty() && this.rawMessages.length > 0;
  }

  isEmptyControlRecord(): boolean {
    return this.isEmpty() && this.messagesWithinOffset.some(({ isControlRecord }) => isControlRecord);
  }

  isEmptyDueToLogCompactedMessages(): boolean {
    return this.rawMessages.length > 0 && this.isEmptyIncludingFiltered();
  }

  firstOffset(): bigint | null {
    if (this.isEmptyIncludingFiltered()) return null;
    return this.messagesWithinOffset[0]?.offset ?? null;
  }

  lastOffset(): bigint {
    if (this.isEmptyDueToLogCompactedMessages()) {
      return this.fetchedOffset;
    }

    if (this.isEmptyIncludingFiltered()) {
      return this.highWatermark - 1n;
    }

    return this.messagesWithinOffset[this.messagesWithinOffset.length - 1]?.offset ?? this.fetchedOffset;
  }

  /** Lag based on the last offset in the batch. */
  offsetLag(): bigint {
    const lastOffsetOfPartition = this.highWatermark - 1n;
    return lastOffsetOfPartition - this.lastOffset();
  }

  /** Lag based on the first offset in the batch. */
  offsetLagLow(): bigint {
    if (this.isEmptyIncludingFiltered()) {
      return 0n;
    }

    const firstOffset = this.firstOffset();
    if (firstOffset === null) return 0n;
    return this.highWatermark - 1n - firstOffset;
  }
}
