import type { Batch } from './batch';
import { SubscriptionState } from './subscription-state';
import type { TopicPartitionOffset, TopicPartitions } from './types';

/** One batch queued or being handled, plus one held by the fetcher until that one settles. */
const MAX_UNSETTLED_BATCHES_PER_PARTITION = 2;

/** Reads a partition's position from the offset manager; `null` while there is none. */
type ReadPosition = (topic: string, partition: number) => bigint | null;

type TopicPartitionsInput = readonly { topic: string; partitions?: number[] }[];

/**
 * Live fetch state of one partition. The fields are plain so that per-record checks stay field
 * reads; only {@link PartitionPipeline} writes them.
 */
export class PartitionState {
  readonly topic: string;
  readonly partition: number;
  /** Whether the partition belongs to the current assignment. */
  isAssigned = false;
  isPaused = false;
  /** Offset of a `seek()` that is not applied yet. */
  pendingSeek: bigint | null = null;
  /** Whether a worker is handling one of the partition's batches. */
  isProcessing = false;
  /** The position as last read from the offset manager; `null` once something may have moved it. */
  position: bigint | null = null;
  /** Batches returned by a fetch that have not left the fetch pipeline yet. */
  unsettledBatches = 0;
  /** Where the next fetch starts while a fetched batch is unsettled; `null` fetches from the position. */
  prefetchOffset: bigint | null = null;
  /** The newest batch that moved `prefetchOffset`. */
  newestPrefetchBatch: Batch | null = null;
  /** The unsettled batch that moved `prefetchOffset` before `newestPrefetchBatch`. */
  previousPrefetchBatch: Batch | null = null;

  constructor(topic: string, partition: number) {
    this.topic = topic;
    this.partition = partition;
  }
}

interface FetchPlanPartition {
  partition: number;
  fetchOffset: bigint;
}

interface FetchPlanTopic {
  topic: string;
  partitions: FetchPlanPartition[];
}

interface FetchPlan {
  topics: FetchPlanTopic[];
  /**
   * A partition was left out because it has the maximum number of unsettled batches. The node
   * has one fetch in flight, so a long poll would also delay that partition's next fetch.
   */
  hasHeldBackPartition: boolean;
}

/**
 * Owns each partition's fetch state for one consumer: which partitions to fetch and from where,
 * whether a fetched batch is still current, and when a held-back partition becomes fetchable.
 * A batch carries its partition's state, resolved once when the fetch response becomes batches.
 */
export class PartitionPipeline {
  readonly #subscriptionState = new SubscriptionState();
  readonly #states = new Map<string, Map<number, PartitionState>>();
  readonly #readPosition: ReadPosition;
  readonly #fetchableWaiters = new Set<() => void>();
  #wakeCount = 0;

  constructor({ readPosition }: { readPosition: ReadPosition }) {
    this.#readPosition = readPosition;
  }

  /** Bumped on every wake, so a fetch can tell that a partition became fetchable while it was in flight. */
  get wakeCount(): number {
    return this.#wakeCount;
  }

  /**
   * Replaces the assignment. Every partition's next fetch starts from its position, and unsettled
   * batches stop counting, so a batch that was never settled cannot hold a partition back for good.
   */
  assign(topicPartitions: readonly TopicPartitions[]): void {
    this.#subscriptionState.assign(topicPartitions);
    for (const states of this.#states.values()) {
      for (const state of states.values()) {
        state.isAssigned = false;
        state.unsettledBatches = 0;
        state.isPaused = this.#subscriptionState.isPaused(state.topic, state.partition);
        this.#resetPrefetch(state);
      }
    }
    for (const { topic, partitions } of topicPartitions) {
      for (const partition of partitions) this.#stateFor(topic, partition).isAssigned = true;
    }
  }

  assigned(): TopicPartitions[] {
    return this.#subscriptionState.assigned();
  }

  paused(): TopicPartitions[] {
    return this.#subscriptionState.paused();
  }

  isAssigned(topic: string, partition: number): boolean {
    return this.#states.get(topic)?.get(partition)?.isAssigned === true;
  }

  /** The partition's state, or `undefined` when it was never assigned or sought. */
  state(topic: string, partition: number): PartitionState | undefined {
    return this.#states.get(topic)?.get(partition);
  }

  pause(topicPartitions: TopicPartitionsInput): void {
    this.#subscriptionState.pause(topicPartitions);
    this.#updatePaused(topicPartitions);
  }

  resume(topicPartitions: TopicPartitionsInput): void {
    this.#subscriptionState.resume(topicPartitions);
    this.#updatePaused(topicPartitions);
    this.#wake();
  }

  /** Records a seek; the next fetch of the partition applies it once no worker is handling it. */
  seek({ topic, partition, offset }: TopicPartitionOffset): void {
    this.#stateFor(topic, partition).pendingSeek = offset;
  }

  /**
   * Applies the pending seeks of `topicPartitions` through `applySeek`. A partition a worker is
   * handling keeps its seek pending, so the worker sees it and stops before the next record.
   */
  async applyPendingSeeks(
    topicPartitions: readonly TopicPartitions[],
    applySeek: (seek: TopicPartitionOffset) => Promise<void>,
  ): Promise<void> {
    for (const { topic, partitions } of topicPartitions) {
      for (const partition of partitions) {
        const state = this.state(topic, partition);
        if (state === undefined || state.pendingSeek === null || state.isProcessing) continue;

        const offset = state.pendingSeek;
        await applySeek({ topic, partition, offset });
        // A newer seek made while this one was applied stays pending.
        if (state.pendingSeek === offset) state.pendingSeek = null;
        this.#resetPrefetch(state);
      }
    }
  }

  /** Makes the partition's next fetch start from its position instead of after its last fetched batch. */
  resetPrefetch(topic: string, partition: number): void {
    const state = this.state(topic, partition);
    if (state) this.#resetPrefetch(state);
  }

  /** {@link resetPrefetch} for every partition, e.g. after a failed fetch. */
  resetPrefetches(): void {
    for (const states of this.#states.values()) {
      for (const state of states.values()) this.#resetPrefetch(state);
    }
  }

  /**
   * Which of `topicPartitions` to fetch now and from which offset. Paused partitions and those
   * without a committed offset are left out, as are partitions with
   * {@link MAX_UNSETTLED_BATCHES_PER_PARTITION} unsettled batches (held back). A partition's next
   * fetch starts after its last fetched batch while that batch is unsettled.
   */
  planFetch(
    topicPartitions: readonly TopicPartitions[],
    hasCommittedOffset: (topic: string, partition: number) => boolean,
  ): FetchPlan {
    const topics: FetchPlanTopic[] = [];
    let hasHeldBackPartition = false;

    for (const { topic, partitions } of topicPartitions) {
      const states = this.#states.get(topic);
      if (states === undefined) continue;

      const planned: FetchPlanPartition[] = [];
      for (const partition of partitions) {
        const state = states.get(partition);
        if (state === undefined || !state.isAssigned || state.isPaused || !hasCommittedOffset(topic, partition)) {
          continue;
        }
        if (state.unsettledBatches >= MAX_UNSETTLED_BATCHES_PER_PARTITION) {
          hasHeldBackPartition = true;
          continue;
        }

        const fetchOffset = state.prefetchOffset ?? this.#refreshPosition(state);
        if (fetchOffset !== null) planned.push({ partition, fetchOffset });
      }
      if (planned.length > 0) topics.push({ topic, partitions: planned });
    }

    return { topics, hasHeldBackPartition };
  }

  /** The state for a partition in a fetch response, or `null` when its records must be dropped. */
  stateForFetchedPartition(topic: string, partition: number): PartitionState | null {
    const state = this.state(topic, partition);
    return state !== undefined && this.#acceptsBatches(state) ? state : null;
  }

  /**
   * Counts `batch` unsettled until {@link settle} and starts its partition's next fetch after it
   * when it continues from the expected offset. A batch fetched from an outdated offset does not
   * move the fetch offset; {@link isBatchCurrent} drops it.
   */
  track(batch: Batch): void {
    const state = batch.partitionState;
    if (state == null) return;

    state.unsettledBatches += 1;
    if (batch.isEmptyIncludingFiltered()) return;

    const expectedOffset = state.prefetchOffset ?? this.#refreshPosition(state);
    if (batch.fetchedOffset !== expectedOffset) return;

    state.prefetchOffset = batch.lastOffset() + 1n;
    state.previousPrefetchBatch = state.newestPrefetchBatch;
    state.newestPrefetchBatch = batch;
  }

  /**
   * Whether `batch` may still be handed to the application: its partition is assigned, not paused,
   * has no pending seek, and its position is where the batch starts. A batch prefetched behind an
   * earlier one that was not fully consumed starts past the position and is therefore dropped.
   */
  isBatchCurrent(batch: Batch): boolean {
    const state = batch.partitionState;
    if (state == null) return true;
    return this.#acceptsBatches(state) && this.#position(state) === batch.fetchedOffset;
  }

  /**
   * Runs once `batch` leaves the fetch pipeline: handled, dropped as stale, discarded, or never
   * enqueued. The partition's next fetch falls back to the consumed position once its newest
   * batch settles, or once an earlier batch settles without being fully consumed.
   */
  settle(batch: Batch): void {
    const state = batch.partitionState;
    if (state == null) return;

    // Zero when the batch was counted before the latest assignment.
    if (state.unsettledBatches > 0) state.unsettledBatches -= 1;
    // Handling the batch may have moved the position; the next check reads it again.
    state.position = null;
    if (batch === state.newestPrefetchBatch) {
      this.#resetPrefetch(state);
    } else if (batch === state.previousPrefetchBatch) {
      state.previousPrefetchBatch = null;
      const isFullyConsumed = this.#position(state) === batch.lastOffset() + 1n;
      if (!isFullyConsumed) this.#resetPrefetch(state);
    }
    this.#wake();
  }

  beginProcessing(batch: Batch): void {
    if (batch.partitionState) batch.partitionState.isProcessing = true;
  }

  endProcessing(batch: Batch): void {
    if (batch.partitionState) batch.partitionState.isProcessing = false;
  }

  /** Waits up to `timeoutMs`, returning early once a partition may be fetchable again. */
  waitForFetchablePartition(timeoutMs: number): Promise<void> {
    return new Promise((resolve) => {
      const done = (): void => {
        clearTimeout(timeoutId);
        this.#fetchableWaiters.delete(done);
        resolve();
      };
      const timeoutId = setTimeout(done, timeoutMs);
      this.#fetchableWaiters.add(done);
    });
  }

  #wake(): void {
    this.#wakeCount += 1;
    for (const done of [...this.#fetchableWaiters]) done();
  }

  #stateFor(topic: string, partition: number): PartitionState {
    let states = this.#states.get(topic);
    if (states === undefined) {
      states = new Map();
      this.#states.set(topic, states);
    }
    let state = states.get(partition);
    if (state === undefined) {
      state = new PartitionState(topic, partition);
      state.isPaused = this.#subscriptionState.isPaused(topic, partition);
      states.set(partition, state);
    }
    return state;
  }

  #updatePaused(topicPartitions: TopicPartitionsInput): void {
    for (const { topic } of topicPartitions) {
      const states = this.#states.get(topic);
      if (states === undefined) continue;
      for (const state of states.values()) {
        state.isPaused = this.#subscriptionState.isPaused(topic, state.partition);
      }
    }
  }

  /** Assigned, not paused, and without a pending seek: its fetched records may still be delivered. */
  #acceptsBatches(state: PartitionState): boolean {
    return state.isAssigned && !state.isPaused && state.pendingSeek === null;
  }

  #position(state: PartitionState): bigint | null {
    return state.position ?? this.#refreshPosition(state);
  }

  #refreshPosition(state: PartitionState): bigint | null {
    state.position = this.#readPosition(state.topic, state.partition);
    return state.position;
  }

  #resetPrefetch(state: PartitionState): void {
    state.position = null;
    state.prefetchOffset = null;
    state.newestPrefetchBatch = null;
    state.previousPrefetchBatch = null;
  }
}
