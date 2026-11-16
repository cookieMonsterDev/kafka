import type { Cluster } from '../../cluster/index';
import { KafkaNonRetriableError } from '../../errors';
import type { InstrumentationEventEmitter } from '../../instrumentation/emitter';
import type { Logger } from '../../loggers/index';
import { runHooks } from '../../utils/run-hooks';
import { COMMIT_OFFSETS } from '../instrumentation-events';
import {
  isByDurationReset,
  listOffsetsQueryForReset,
  resolveAutoOffsetReset,
  type AutoOffsetReset,
  type TopicOffsetConfiguration,
} from '../offset-reset';
import type {
  ConsumerHooks,
  MemberAssignment,
  Offsets,
  OffsetsByTopicPartition,
  TopicPartition,
  TopicOffsets,
  TopicPartitionOffset,
} from '../types';
import type { CommitTarget } from './commit-target';
import { initializeConsumerOffsets } from './initialize-consumer-offsets';
import { isInvalidOffset } from './is-invalid-offset';
import { ResolvedOffsets } from './resolved-offsets';

export interface OffsetTrackerOptions {
  cluster: Cluster;
  target: CommitTarget;
  topicConfigurations: Record<string, TopicOffsetConfiguration>;
  instrumentationEmitter: InstrumentationEventEmitter;
  /** Whether a group member's seek is committed at once instead of left uncommitted. */
  autoCommit: boolean;
  autoCommitInterval: number | null;
  autoCommitThreshold: number | null;
  logger: Logger;
  /** Ordered async `onCommit` hook. See {@link ConsumerHooks}. */
  hooks?: ConsumerHooks;
}

/**
 * The consumer's offsets for one run: the committed offset and the last resolved (processed)
 * offset of each assigned partition. It is told about each assignment change, keeps the offsets of
 * retained partitions, and forgets revoked ones. Commits go to its {@link CommitTarget}.
 *
 * Kafka's committed offset is the next offset to read, not the last consumed one, so
 * `resolveOffset({ offset })` stores `offset + 1n`.
 */
export class OffsetTracker {
  readonly #cluster: Cluster;
  readonly #target: CommitTarget;
  readonly #topicConfigurations: Record<string, TopicOffsetConfiguration>;
  readonly #instrumentationEmitter: InstrumentationEventEmitter;
  readonly #autoCommit: boolean;
  readonly #autoCommitInterval: number | null;
  readonly #autoCommitThreshold: number | null;
  readonly #logger: Logger;
  readonly #hooks: ConsumerHooks | undefined;
  readonly #resolvedOffsets = new ResolvedOffsets();
  #assignment: MemberAssignment = Object.create(null) as MemberAssignment;
  #lastCommit = Date.now();

  constructor({
    cluster,
    target,
    topicConfigurations,
    instrumentationEmitter,
    autoCommit,
    autoCommitInterval,
    autoCommitThreshold,
    logger,
    hooks,
  }: OffsetTrackerOptions) {
    this.#cluster = cluster;
    this.#target = target;
    this.#topicConfigurations = topicConfigurations;
    this.#instrumentationEmitter = instrumentationEmitter;
    this.#autoCommit = autoCommit;
    this.#autoCommitInterval = autoCommitInterval;
    this.#autoCommitThreshold = autoCommitThreshold;
    this.#logger = logger.namespace('OffsetTracker');
    this.#hooks = hooks;
  }

  /**
   * Installs the member's new assignment. Partitions in both the old and the new assignment keep
   * their offsets. Revoked partitions lose theirs, and gained ones look their committed offset up
   * again: it may have moved while this consumer did not own the partition, and an earlier run on
   * the same client may have left an entry in the cluster's record.
   */
  updateAssignment(assignment: MemberAssignment): void {
    const previousAssignment = this.#assignment;
    this.#assignment = assignment;
    const isRetained = (topic: string, partition: number): boolean =>
      previousAssignment[topic]?.includes(partition) === true && assignment[topic]?.includes(partition) === true;

    for (const changedAssignment of [previousAssignment, assignment]) {
      for (const topic of Object.keys(changedAssignment)) {
        for (const partition of changedAssignment[topic] ?? []) {
          if (!isRetained(topic, partition)) this.#target.deleteCommittedOffset(topic, partition);
        }
      }
    }

    // A batch of a revoked partition can still resolve offsets after it left the assignment.
    const resolvedOffsets = this.#resolvedOffsets.snapshot();
    for (const topic of Object.keys(resolvedOffsets)) {
      for (const partition of Object.keys(resolvedOffsets[topic] ?? {})) {
        if (!isRetained(topic, Number(partition))) this.#forgetResolvedOffset(topic, Number(partition));
      }
    }
  }

  /** Whether the partition's committed offset (in assign mode, its starting position) is known. */
  hasCommittedOffset(topic: string, partition: number): boolean {
    return this.#target.committedOffset(topic, partition) != null;
  }

  nextOffset(topic: string, partition: number): bigint {
    const resolvedOffsets = this.#resolvedOffsets.snapshot();
    let offset = resolvedOffsets[topic]?.[partition];
    if (offset === undefined) {
      const committed = this.#target.committedOffset(topic, partition);
      // Do not cache a fallback 0n: `position()` can run after join and before the first
      // fetch's resolveOffsets(), and a cached 0n would then pin Fetch to the log start.
      if (committed === undefined) {
        return 0n;
      }
      (resolvedOffsets[topic] ??= {})[partition] = committed;
      offset = committed;
    }

    return isInvalidOffset(offset) ? 0n : offset;
  }

  resetOffset({ topic, partition }: TopicPartition): void {
    const resolvedOffsets = this.#resolvedOffsets.snapshot();
    (resolvedOffsets[topic] ??= {})[partition] = this.#target.committedOffset(topic, partition) ?? 0n;
  }

  resolveOffset({ topic, partition, offset }: TopicPartitionOffset): void {
    this.#resolvedOffsets.resolve(topic, partition, offset);
  }

  /** How many offsets were resolved past the committed ones, across the assignment. */
  countResolvedOffsets(): bigint {
    const resolvedOffsets = this.#resolvedOffsets.snapshot();

    let sum = 0n;
    for (const topic of Object.keys(this.#assignment)) {
      const resolvedTopic = resolvedOffsets[topic];
      if (!resolvedTopic) continue;
      for (const partition of Object.keys(resolvedTopic)) {
        const resolved = resolvedTopic[partition] ?? 0n;
        const committed = this.#target.committedOffset(topic, Number(partition));
        sum += committed === undefined || isInvalidOffset(committed) ? resolved : resolved - committed;
      }
    }

    return sum;
  }

  async setDefaultOffset({ topic, partition }: TopicPartition): Promise<void> {
    const reset = resolveAutoOffsetReset(this.#topicConfigurations[topic]);
    if (reset === 'none') {
      throw new KafkaNonRetriableError(
        `Offset reset policy is none; no committed offset for topic ${topic} partition ${partition}`,
      );
    }

    const { groupId } = this.#target;
    // Without a group, the next resolveOffsets() applies the reset policy locally.
    if (groupId !== null) {
      const defaultOffset = await this.#defaultOffsetFor(topic, partition, reset);
      await this.#sendCommit(groupId, [{ topic, partitions: [{ partition, offset: defaultOffset }] }]);
    }

    this.#clearOffsets(topic, partition);
  }

  /**
   * Moves an assigned partition's position; a partition outside the assignment is ignored. A group
   * member with auto-commit commits the offset at once; without auto-commit, the offset is left
   * resolved for the next commit. An assign-mode seek only sets the local starting position.
   */
  async seek({ topic, partition, offset }: TopicPartitionOffset): Promise<void> {
    if (!this.#assignment[topic]?.includes(partition)) {
      return;
    }

    if (!this.#target.isGroupMember) {
      this.#target.setCommittedOffset(topic, partition, offset);
      this.#forgetResolvedOffset(topic, partition);
      return;
    }

    if (!this.#autoCommit) {
      // `resolveOffset` adds one, so this lands on the requested offset.
      this.resolveOffset({ topic, partition, offset: offset - 1n });
      return;
    }

    await this.#sendCommit(this.#requireGroupId(), [{ topic, partitions: [{ partition, offset }] }]);
    this.#clearOffsets(topic, partition);
  }

  /**
   * Commits when an `autoCommitInterval` or `autoCommitThreshold` is due, and on every call when
   * neither is set.
   */
  async commitOffsetsIfNecessary(): Promise<void> {
    if (this.#autoCommitInterval == null && this.#autoCommitThreshold == null) {
      await this.commitOffsets();
      return;
    }

    const now = Date.now();
    const timeoutReached = this.#autoCommitInterval != null && now >= this.#lastCommit + this.#autoCommitInterval;
    const thresholdReached =
      this.#autoCommitThreshold != null && this.countResolvedOffsets() >= BigInt(this.#autoCommitThreshold);

    if (timeoutReached || thresholdReached) {
      await this.commitOffsets();
    }
  }

  uncommittedOffsets(): OffsetsByTopicPartition {
    const resolvedOffsets = this.#resolvedOffsets.snapshot();

    const topicsWithPartitionsToCommit = Object.keys(this.#assignment)
      .map((topic) => {
        const resolvedTopic = resolvedOffsets[topic] ?? {};
        const partitions = Object.keys(resolvedTopic)
          .map((partition) => ({
            partition: Number(partition),
            offset: resolvedTopic[partition] ?? 0n,
          }))
          .filter(({ partition, offset }) => offset !== this.#target.committedOffset(topic, partition) && offset >= 0n);

        return { topic, partitions };
      })
      .filter(({ partitions }) => partitions.length > 0);

    return { topics: topicsWithPartitionsToCommit };
  }

  /**
   * Commits `offsets`, or every uncommitted resolved offset when none are given. In assign mode,
   * which commits only when asked to, an empty list also commits every uncommitted offset.
   */
  async commitOffsets(offsets?: Offsets): Promise<void> {
    const groupId = this.#requireGroupId();
    const commitsUncommitted = offsets === undefined || (offsets.topics.length === 0 && !this.#target.isGroupMember);
    const topics = commitsUncommitted ? this.uncommittedOffsets().topics : offsets.topics;

    if (topics.length === 0) {
      this.#lastCommit = Date.now();
      return;
    }

    const payload = { groupId, ...this.#target.commitIdentity(), topics };

    try {
      const coordinator = await this.#target.coordinator();
      await coordinator.offsetCommit(payload);
      this.#instrumentationEmitter.emit(COMMIT_OFFSETS, payload);
      this.#markCommitted(topics);

      this.#lastCommit = Date.now();
      if (this.#hooks?.onCommit?.length) {
        await runHooks(this.#hooks.onCommit, payload, 'onCommit', this.#logger);
      }
    } catch (e) {
      const error = e as { type?: string };
      if (error.type === 'NOT_COORDINATOR_FOR_GROUP') {
        await this.#cluster.refreshMetadata();
      }

      if (this.#hooks?.onCommit?.length) {
        await runHooks(this.#hooks.onCommit, { ...payload, error: e }, 'onCommit', this.#logger);
      }
      throw e;
    }
  }

  /**
   * Looks up the committed offset of each assigned partition that has none yet, falling back to
   * `autoOffsetReset` where the group has none. Without a group, every such partition takes the
   * reset policy.
   */
  async resolveOffsets(): Promise<void> {
    const pendingPartitions = Object.keys(this.#assignment)
      .map((topic) => ({
        topic,
        partitions: (this.#assignment[topic] ?? [])
          .filter((partition) => isInvalidOffset(this.#target.committedOffset(topic, partition)))
          .map((partition) => ({ partition })),
      }))
      .filter((t) => t.partitions.length > 0);

    if (pendingPartitions.length === 0) {
      return;
    }

    let consumerOffsets: TopicOffsets[];
    const { groupId } = this.#target;
    if (groupId !== null) {
      const coordinator = await this.#target.coordinator();
      ({ responses: consumerOffsets } = await coordinator.offsetFetch({ groupId, topics: pendingPartitions }));
    } else {
      consumerOffsets = pendingPartitions.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({ partition, offset: -1n })),
      }));
    }

    const unresolvedPartitions = consumerOffsets.map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.filter(({ offset }) => isInvalidOffset(offset)).map(({ partition }) => ({ partition })),
    }));

    let offsets: readonly TopicOffsets[] = consumerOffsets;

    if (unresolvedPartitions.some((t) => t.partitions.length > 0)) {
      const resetByTopic: Record<string, AutoOffsetReset> = Object.fromEntries(
        unresolvedPartitions.map(({ topic }) => [topic, resolveAutoOffsetReset(this.#topicConfigurations[topic])]),
      );

      const topicsForListOffsets = unresolvedPartitions
        .map((t) => {
          const reset = resetByTopic[t.topic];
          if (t.partitions.length === 0 || reset == null) return null;
          return listOffsetsQueryForReset(t.topic, t.partitions, reset);
        })
        .filter((query): query is NonNullable<typeof query> => query != null);

      const topicOffsets =
        topicsForListOffsets.length > 0 ? await this.#cluster.fetchTopicsOffset(topicsForListOffsets) : [];
      offsets = initializeConsumerOffsets(consumerOffsets, topicOffsets, resetByTopic);
    }

    this.#markCommitted(offsets);
  }

  #requireGroupId(): string {
    const { groupId } = this.#target;
    if (groupId === null) {
      throw new KafkaNonRetriableError(
        'Cannot commit offsets in assign() mode without a configured groupId. Pass `groupId` to `kafka.consumer(...)` to enable commitOffsets().',
      );
    }
    return groupId;
  }

  /**
   * Sends one OffsetCommit, tagged with the member identity current at the time it is sent. Used
   * for a seek or an offset reset, which moves the position rather than committing processed
   * offsets, so it is not reported through `COMMIT_OFFSETS` or `onCommit`.
   */
  async #sendCommit(groupId: string, topics: TopicOffsets[]): Promise<void> {
    const coordinator = await this.#target.coordinator();
    await coordinator.offsetCommit({ groupId, ...this.#target.commitIdentity(), topics });
  }

  #markCommitted(topics: readonly TopicOffsets[]): void {
    for (const { topic, partitions } of topics) {
      for (const { partition, offset } of partitions) {
        this.#target.setCommittedOffset(topic, partition, offset);
      }
    }
  }

  #forgetResolvedOffset(topic: string, partition: number): void {
    const resolvedTopic = this.#resolvedOffsets.snapshot()[topic];
    if (resolvedTopic) delete resolvedTopic[partition];
  }

  #clearOffsets(topic: string, partition: number): void {
    this.#target.deleteCommittedOffset(topic, partition);
    this.#forgetResolvedOffset(topic, partition);
  }

  async #defaultOffsetFor(topic: string, partition: number, reset: AutoOffsetReset): Promise<bigint> {
    if (isByDurationReset(reset)) {
      const query = listOffsetsQueryForReset(topic, [{ partition }], reset);
      const [result] = query ? await this.#cluster.fetchTopicsOffset([query]) : [];
      const offset = result?.partitions.find((entry) => entry.partition === partition)?.offset;
      if (offset == null) {
        throw new KafkaNonRetriableError(`No ListOffsets result for topic ${topic} partition ${partition}`);
      }
      return offset;
    }

    return this.#cluster.defaultOffset({ fromBeginning: reset === 'earliest' });
  }
}
