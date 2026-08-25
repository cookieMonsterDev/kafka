import type { Broker } from '../../broker/index';
import type { Cluster } from '../../cluster/index';
import { KafkaNonRetriableError } from '../../errors';
import type { InstrumentationEventEmitter } from '../../instrumentation/emitter';
import { COMMIT_OFFSETS } from '../instrumentation-events';
import {
  isByDurationReset,
  listOffsetsQueryForReset,
  resolveAutoOffsetReset,
  type AutoOffsetReset,
  type TopicOffsetConfiguration,
} from '../offset-reset';
import type {
  MemberAssignment,
  Offsets,
  OffsetsByTopicPartition,
  TopicPartition,
  TopicPartitionOffset,
} from '../types';
import { initializeConsumerOffsets } from './initialize-consumer-offsets';
import { isInvalidOffset } from './is-invalid-offset';
import type { OffsetManagerHandle } from './offset-manager-handle';

/**
 * `OffsetCommit`/`OffsetFetch` outside of any consumer-group membership use these sentinels -
 * Kafka's "simple"/standalone consumer convention. The broker skips generation and membership
 * validation when `groupGenerationId` is `-1` and `memberId` is empty.
 */
const STANDALONE_GENERATION_ID = -1;
const STANDALONE_MEMBER_ID = '';

function indexTopics(topics: readonly string[]): Record<string, Record<string, bigint>> {
  return topics.reduce<Record<string, Record<string, bigint>>>((obj, topic) => {
    obj[topic] = {};
    return obj;
  }, {});
}

export interface AssignedOffsetManagerOptions {
  cluster: Cluster;
  /** `null` when the consumer has no configured `groupId`; `commitOffsets` then always rejects. */
  groupId: string | null;
  assignment: MemberAssignment;
  topicConfigurations: Record<string, TopicOffsetConfiguration>;
  instrumentationEmitter: InstrumentationEventEmitter;
}

/**
 * Offset tracking for `consumer.assign()` (no group membership, no JoinGroup/SyncGroup).
 *
 * Initial position, per partition, decided the first time it is needed (and cached after):
 * 1. An explicit `consumer.seek()` call, applied by `ConsumerGroup.seekOffsets` exactly like
 *    subscribe-mode, always wins.
 * 2. Otherwise, if the consumer has a `groupId` configured, the group's committed offset
 *    (`OffsetFetch`) for that partition, when one exists.
 * 3. Otherwise, `autoOffsetReset` (default `latest`) resolved via `ListOffsets` - the same
 *    fallback subscribe-mode uses when a group has no committed offset yet.
 *
 * `commitOffsets` requires a configured `groupId` and throws a clear error otherwise. When a
 * `groupId` is configured, commits are sent as a standalone consumer (`groupGenerationId: -1`,
 * `memberId: ''`), so the broker does not validate them against a running group's generation or
 * membership.
 */
export class AssignedOffsetManager implements OffsetManagerHandle {
  cluster: Cluster;
  groupId: string | null;
  assignment: MemberAssignment;
  topicConfigurations: Record<string, TopicOffsetConfiguration>;
  instrumentationEmitter: InstrumentationEventEmitter;
  topics: string[];
  resolvedOffsets: Record<string, Record<string, bigint>>;
  #basePositions: Record<string, Record<string, bigint>>;
  #coordinator: Broker | null = null;

  constructor({
    cluster,
    groupId,
    assignment,
    topicConfigurations,
    instrumentationEmitter,
  }: AssignedOffsetManagerOptions) {
    this.cluster = cluster;
    this.groupId = groupId;
    this.assignment = assignment;
    this.topicConfigurations = topicConfigurations;
    this.instrumentationEmitter = instrumentationEmitter;
    this.topics = Object.keys(assignment);
    this.resolvedOffsets = indexTopics(this.topics);
    this.#basePositions = indexTopics(this.topics);
  }

  /**
   * Update the fixed assignment in place, e.g. after a metadata refresh recomputed partition
   * leadership. Existing partitions keep whatever position they already resolved/consumed;
   * only newly seen partitions start with a blank slate.
   */
  updateAssignment(assignment: MemberAssignment): void {
    this.assignment = assignment;
    this.topics = Object.keys(assignment);
    for (const topic of this.topics) {
      this.resolvedOffsets[topic] ??= {};
      this.#basePositions[topic] ??= {};
    }
  }

  nextOffset(topic: string, partition: number): bigint {
    const resolvedTopic = (this.resolvedOffsets[topic] ??= {});
    if (resolvedTopic[partition] === undefined) {
      resolvedTopic[partition] = this.#basePositions[topic]?.[partition] ?? 0n;
    }

    let offset = resolvedTopic[partition];
    if (isInvalidOffset(offset)) {
      offset = 0n;
    }

    return offset ?? 0n;
  }

  async getCoordinator(): Promise<Broker> {
    if (!this.groupId) {
      throw new KafkaNonRetriableError(
        'Cannot reach a group coordinator in assign() mode without a configured groupId.',
      );
    }

    if (!this.#coordinator || !this.#coordinator.isConnected()) {
      this.#coordinator = await this.cluster.findGroupCoordinator({ groupId: this.groupId });
    }

    return this.#coordinator;
  }

  resetOffset({ topic, partition }: TopicPartition): void {
    const resolvedTopic = (this.resolvedOffsets[topic] ??= {});
    resolvedTopic[partition] = this.#basePositions[topic]?.[partition] ?? 0n;
  }

  resolveOffset({ topic, partition, offset }: TopicPartitionOffset): void {
    const resolvedTopic = (this.resolvedOffsets[topic] ??= {});
    resolvedTopic[partition] = offset + 1n;
  }

  countResolvedOffsets(): bigint {
    const subtractOffsets = (resolvedOffset: bigint | undefined, baseOffset: bigint | undefined): bigint => {
      const resolved = resolvedOffset ?? 0n;
      return isInvalidOffset(baseOffset) ? resolved : resolved - (baseOffset ?? 0n);
    };

    let sum = 0n;
    for (const topic of this.topics) {
      const resolvedTopicOffsets = this.resolvedOffsets[topic] ?? {};
      const baseTopicOffsets = this.#basePositions[topic] ?? {};
      for (const partition of Object.keys(resolvedTopicOffsets)) {
        sum += subtractOffsets(resolvedTopicOffsets[partition], baseTopicOffsets[partition]);
      }
    }

    return sum;
  }

  /**
   * Overrides the local position directly. Unlike group mode, this never round-trips through
   * the broker: assign-mode never auto-commits, so there is no group offset to keep in sync.
   */
  async seek({ topic, partition, offset }: TopicPartitionOffset): Promise<void> {
    const assignedPartitions = this.assignment[topic];
    if (!assignedPartitions || !assignedPartitions.includes(partition)) {
      return;
    }

    (this.#basePositions[topic] ??= {})[partition] = offset;
    const resolvedTopic = this.resolvedOffsets[topic];
    if (resolvedTopic) delete resolvedTopic[partition];
  }

  async setDefaultOffset({ topic, partition }: TopicPartition): Promise<void> {
    const reset = resolveAutoOffsetReset(this.topicConfigurations[topic]);
    if (reset === 'none') {
      throw new KafkaNonRetriableError(
        `Offset reset policy is none; no committed offset for topic ${topic} partition ${partition}`,
      );
    }

    if (this.groupId) {
      const defaultOffset = await this.#defaultOffsetFor(topic, partition, reset);
      const coordinator = await this.getCoordinator();
      await coordinator.offsetCommit({
        groupId: this.groupId,
        memberId: STANDALONE_MEMBER_ID,
        groupGenerationId: STANDALONE_GENERATION_ID,
        topics: [{ topic, partitions: [{ partition, offset: defaultOffset }] }],
      });
    }

    this.clearOffsets({ topic, partition });
  }

  async commitOffsetsIfNecessary(): Promise<void> {
    await this.commitOffsets();
  }

  uncommittedOffsets(): OffsetsByTopicPartition {
    const topicsWithPartitionsToCommit = this.topics
      .map((topic) => {
        const resolvedTopic = this.resolvedOffsets[topic] ?? {};
        const baseTopic = this.#basePositions[topic] ?? {};
        const partitions = Object.keys(resolvedTopic)
          .map((partition) => ({
            partition: Number(partition),
            offset: resolvedTopic[partition] ?? 0n,
          }))
          .filter(({ partition, offset }) => offset !== baseTopic[partition] && offset >= 0n);

        return { topic, partitions };
      })
      .filter(({ partitions }) => partitions.length > 0);

    return { topics: topicsWithPartitionsToCommit };
  }

  async commitOffsets(offsets: Offsets = { topics: [] }): Promise<void> {
    if (!this.groupId) {
      throw new KafkaNonRetriableError(
        'Cannot commit offsets in assign() mode without a configured groupId. Pass `groupId` to `kafka.consumer(...)` to enable commitOffsets().',
      );
    }

    const topics = offsets.topics.length > 0 ? offsets.topics : this.uncommittedOffsets().topics;
    if (topics.length === 0) {
      return;
    }

    const payload = {
      groupId: this.groupId,
      memberId: STANDALONE_MEMBER_ID,
      groupGenerationId: STANDALONE_GENERATION_ID,
      topics,
    };

    try {
      const coordinator = await this.getCoordinator();
      await coordinator.offsetCommit(payload);
      this.instrumentationEmitter.emit(COMMIT_OFFSETS, payload);

      for (const { topic, partitions } of topics) {
        const baseTopic = (this.#basePositions[topic] ??= {});
        for (const { partition, offset } of partitions) {
          baseTopic[partition] = offset;
        }
      }
    } catch (e) {
      const error = e as { type?: string };
      if (error.type === 'NOT_COORDINATOR_FOR_GROUP') {
        await this.cluster.refreshMetadata();
      }

      throw e;
    }
  }

  async resolveOffsets(): Promise<void> {
    const pendingPartitions = this.topics
      .map((topic) => ({
        topic,
        partitions: (this.assignment[topic] ?? [])
          .filter((partition) => isInvalidOffset(this.#basePositions[topic]?.[partition]))
          .map((partition) => ({ partition })),
      }))
      .filter((t) => t.partitions.length > 0);

    if (pendingPartitions.length === 0) {
      return;
    }

    let consumerOffsets: { topic: string; partitions: { partition: number; offset: bigint }[] }[];

    if (this.groupId) {
      const coordinator = await this.getCoordinator();
      ({ responses: consumerOffsets } = await coordinator.offsetFetch({
        groupId: this.groupId,
        topics: pendingPartitions,
      }));
    } else {
      // No groupId configured: skip OffsetFetch entirely and resolve straight from autoOffsetReset.
      consumerOffsets = pendingPartitions.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({ partition, offset: -1n })),
      }));
    }

    const unresolvedPartitions = consumerOffsets.map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.filter(({ offset }) => isInvalidOffset(offset)).map(({ partition }) => ({ partition })),
    }));

    const hasUnresolvedPartitions = unresolvedPartitions.some((t) => t.partitions.length > 0);

    let offsets: { topic: string; partitions: { partition: number; offset: bigint }[] }[] = consumerOffsets.map(
      ({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition, offset }) => ({ partition, offset })),
      }),
    );

    if (hasUnresolvedPartitions) {
      const resetByTopic: Record<string, AutoOffsetReset> = Object.fromEntries(
        unresolvedPartitions.map(({ topic }) => [topic, resolveAutoOffsetReset(this.topicConfigurations[topic])]),
      );

      const topicsForListOffsets = unresolvedPartitions
        .map((t) => {
          const reset = resetByTopic[t.topic];
          if (t.partitions.length === 0 || reset == null) return null;
          return listOffsetsQueryForReset(t.topic, t.partitions, reset);
        })
        .filter((query): query is NonNullable<typeof query> => query != null);

      const topicOffsets =
        topicsForListOffsets.length > 0 ? await this.cluster.fetchTopicsOffset(topicsForListOffsets) : [];
      offsets = initializeConsumerOffsets(consumerOffsets, topicOffsets, resetByTopic);
    }

    for (const { topic, partitions } of offsets) {
      const baseTopic = (this.#basePositions[topic] ??= {});
      for (const { partition, offset } of partitions) {
        baseTopic[partition] = offset;
      }
    }
  }

  clearOffsets({ topic, partition }: TopicPartition): void {
    const base = this.#basePositions[topic];
    if (base) delete base[partition];
    const resolved = this.resolvedOffsets[topic];
    if (resolved) delete resolved[partition];
  }

  clearAllOffsets(): void {
    this.#basePositions = indexTopics(this.topics);
    this.resolvedOffsets = indexTopics(this.topics);
  }

  committedOffsets(): Record<string, Record<string, bigint>> {
    return this.#basePositions;
  }

  async #defaultOffsetFor(topic: string, partition: number, reset: AutoOffsetReset): Promise<bigint> {
    if (isByDurationReset(reset)) {
      const query = listOffsetsQueryForReset(topic, [{ partition }], reset);
      const [result] = query ? await this.cluster.fetchTopicsOffset([query]) : [];
      const offset = result?.partitions.find((entry) => entry.partition === partition)?.offset;
      if (offset == null) {
        throw new KafkaNonRetriableError(`No ListOffsets result for topic ${topic} partition ${partition}`);
      }
      return offset;
    }

    return this.cluster.defaultOffset({ fromBeginning: reset === 'earliest' });
  }
}
