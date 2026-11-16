import type { ShareAcquiredRecords, ShareFetchAcknowledgementBatchInput } from '../protocol/requests/share-fetch/index';
import { SHARE_ACKNOWLEDGE_TYPE, type ShareAcknowledgeType } from './acknowledge-types';
import type { ShareAcknowledgeTopicPayload } from './instrumentation-events';

/** An acquired range and the acknowledgement decided for it. */
export interface PendingAcknowledgement {
  firstOffset: bigint;
  lastOffset: bigint;
  acknowledgeType: ShareAcknowledgeType;
}

/** The acknowledgements taken from the ledger for one partition of a request. */
export interface TakenAcknowledgements {
  topic: string;
  partition: number;
  acknowledgements: PendingAcknowledgement[];
}

/**
 * Acknowledgements decided but not yet sent, keyed by partition. Handlers queue them; a request to
 * the partition's leader takes them; if the request never went out, they are restored for the next
 * one. Acknowledgements taken for a request that went out, or for a share session that ended, are
 * settled by not restoring them.
 */
export class AcknowledgementLedger {
  readonly #pendingByTopic = new Map<string, Map<number, PendingAcknowledgement[]>>();

  /** Queues `acknowledgeType` for every acquired range; synchronous, so a handler never waits on it. */
  queue(
    topic: string,
    partition: number,
    acquiredRecords: readonly ShareAcquiredRecords[],
    acknowledgeType: ShareAcknowledgeType,
  ): void {
    if (acquiredRecords.length === 0) return;
    const pendingByPartition = this.#pendingByPartitionFor(topic);
    let pending = pendingByPartition.get(partition);
    if (!pending) {
      pending = [];
      pendingByPartition.set(partition, pending);
    }
    for (const { firstOffset, lastOffset } of acquiredRecords) {
      pending.push({ firstOffset, lastOffset, acknowledgeType });
    }
  }

  /** Removes and returns the partition's pending acknowledgements, or `undefined` when it has none. */
  take(topic: string, partition: number): PendingAcknowledgement[] | undefined {
    const pendingByPartition = this.#pendingByTopic.get(topic);
    const pending = pendingByPartition?.get(partition);
    if (pending) pendingByPartition?.delete(partition);
    return pending;
  }

  /** Puts back acknowledgements taken for a request that never went out, ahead of any queued since. */
  restore(taken: readonly TakenAcknowledgements[]): void {
    for (const { topic, partition, acknowledgements } of taken) {
      const queuedSince = this.take(topic, partition) ?? [];
      this.#pendingByPartitionFor(topic).set(partition, [...acknowledgements, ...queuedSince]);
    }
  }

  clear(): void {
    this.#pendingByTopic.clear();
  }

  #pendingByPartitionFor(topic: string): Map<number, PendingAcknowledgement[]> {
    let pendingByPartition = this.#pendingByTopic.get(topic);
    if (!pendingByPartition) {
      pendingByPartition = new Map();
      this.#pendingByTopic.set(topic, pendingByPartition);
    }
    return pendingByPartition;
  }
}

/** The ShareFetch or ShareAcknowledge request batches for a partition's acknowledgements. */
export function toAcknowledgementBatches(
  acknowledgements: readonly PendingAcknowledgement[],
): ShareFetchAcknowledgementBatchInput[] {
  return acknowledgements.map(({ firstOffset, lastOffset, acknowledgeType }) => ({
    firstOffset,
    lastOffset,
    acknowledgeTypes: [acknowledgeType],
  }));
}

/** Whether a request carrying `taken` must be marked `isRenewAck`. */
export function hasRenewAcknowledgement(taken: readonly TakenAcknowledgements[]): boolean {
  return taken.some(({ acknowledgements }) =>
    acknowledgements.some(({ acknowledgeType }) => acknowledgeType === SHARE_ACKNOWLEDGE_TYPE.RENEW),
  );
}

/** Groups taken acknowledgements into the `ACKNOWLEDGE` event's per-topic shape. */
export function toAcknowledgeEventTopics(taken: readonly TakenAcknowledgements[]): ShareAcknowledgeTopicPayload[] {
  const partitionsByTopic = new Map<string, ShareAcknowledgeTopicPayload['partitions']>();
  for (const { topic, partition, acknowledgements } of taken) {
    let partitions = partitionsByTopic.get(topic);
    if (!partitions) {
      partitions = [];
      partitionsByTopic.set(topic, partitions);
    }
    for (const { firstOffset, lastOffset, acknowledgeType } of acknowledgements) {
      partitions.push({ partition, firstOffset, lastOffset, acknowledgeType });
    }
  }
  return [...partitionsByTopic].map(([topic, partitions]) => ({ topic, partitions }));
}
