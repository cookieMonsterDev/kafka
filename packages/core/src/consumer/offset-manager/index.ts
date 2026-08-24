import type { Broker } from '../../broker/index';
import type { Cluster } from '../../cluster/index';
import { KafkaNonRetriableError } from '../../errors';
import type { InstrumentationEventEmitter } from '../../instrumentation/emitter';
import type { Logger } from '../../loggers/index';
import { runHooks } from '../../utils/run-hooks';
import { COMMIT_OFFSETS } from '../instrumentation-events';
import { resolveAutoOffsetReset, type AutoOffsetReset, type TopicOffsetConfiguration } from '../offset-reset';
import type {
  ConsumerHooks,
  MemberAssignment,
  Offsets,
  OffsetsByTopicPartition,
  TopicPartition,
  TopicPartitionOffset,
} from '../types';
import { initializeConsumerOffsets } from './initialize-consumer-offsets';
import { isInvalidOffset } from './is-invalid-offset';
import type { OffsetManagerHandle } from './offset-manager-handle';

export type { OffsetManagerHandle } from './offset-manager-handle';

function indexTopics(topics: readonly string[]): Record<string, Record<string, bigint>> {
  return topics.reduce<Record<string, Record<string, bigint>>>((obj, topic) => {
    obj[topic] = {};
    return obj;
  }, {});
}

export interface OffsetManagerOptions {
  cluster: Cluster;
  coordinator: Broker;
  memberAssignment: MemberAssignment;
  autoCommit: boolean;
  autoCommitInterval: number | null;
  autoCommitThreshold: number | null;
  topicConfigurations: Record<string, TopicOffsetConfiguration>;
  instrumentationEmitter: InstrumentationEventEmitter;
  groupId: string;
  generationId: number;
  memberId: string;
  logger: Logger;
  /** Ordered async `onCommit` hook. See {@link ConsumerHooks}. */
  hooks?: ConsumerHooks;
}

/**
 * Kafka's committed offset is the next offset to read, not the last consumed one.
 * `resolveOffset({ offset })` therefore stores `offset + 1n`.
 */
export class OffsetManager implements OffsetManagerHandle {
  cluster: Cluster;
  coordinator: Broker;
  memberAssignment: MemberAssignment;
  topicConfigurations: Record<string, TopicOffsetConfiguration>;
  instrumentationEmitter: InstrumentationEventEmitter;
  groupId: string;
  generationId: number;
  memberId: string;
  autoCommit: boolean;
  autoCommitInterval: number | null;
  autoCommitThreshold: number | null;
  lastCommit: number;
  topics: string[];
  resolvedOffsets: Record<string, Record<string, bigint>>;
  logger: Logger;
  hooks: ConsumerHooks | undefined;
  #committedOffsets: Record<string, Record<string, bigint>> | undefined;

  constructor({
    cluster,
    coordinator,
    memberAssignment,
    autoCommit,
    autoCommitInterval,
    autoCommitThreshold,
    topicConfigurations,
    instrumentationEmitter,
    groupId,
    generationId,
    memberId,
    logger,
    hooks,
  }: OffsetManagerOptions) {
    this.cluster = cluster;
    this.coordinator = coordinator;
    this.memberAssignment = memberAssignment;
    this.topicConfigurations = topicConfigurations;
    this.instrumentationEmitter = instrumentationEmitter;
    this.groupId = groupId;
    this.generationId = generationId;
    this.memberId = memberId;
    this.autoCommit = autoCommit;
    this.autoCommitInterval = autoCommitInterval;
    this.autoCommitThreshold = autoCommitThreshold;
    this.lastCommit = Date.now();
    this.topics = Object.keys(memberAssignment);
    this.resolvedOffsets = {};
    this.logger = logger.namespace('OffsetManager');
    this.hooks = hooks;
    this.clearAllOffsets();
  }

  nextOffset(topic: string, partition: number): bigint {
    const resolvedTopic = (this.resolvedOffsets[topic] ??= {});
    if (resolvedTopic[partition] === undefined) {
      resolvedTopic[partition] = this.committedOffsets()[topic]?.[partition] ?? 0n;
    }

    let offset = resolvedTopic[partition];
    if (isInvalidOffset(offset)) {
      offset = 0n;
    }

    return offset ?? 0n;
  }

  async getCoordinator(): Promise<Broker> {
    if (!this.coordinator.isConnected()) {
      this.coordinator = await this.cluster.findBroker({ nodeId: String(this.coordinator.nodeId) });
    }

    return this.coordinator;
  }

  resetOffset({ topic, partition }: TopicPartition): void {
    const resolvedTopic = (this.resolvedOffsets[topic] ??= {});
    resolvedTopic[partition] = this.committedOffsets()[topic]?.[partition] ?? 0n;
  }

  resolveOffset({ topic, partition, offset }: TopicPartitionOffset): void {
    const resolvedTopic = (this.resolvedOffsets[topic] ??= {});
    resolvedTopic[partition] = offset + 1n;
  }

  countResolvedOffsets(): bigint {
    const committedOffsets = this.committedOffsets();

    const subtractOffsets = (resolvedOffset: bigint | undefined, committedOffset: bigint | undefined): bigint => {
      const resolved = resolvedOffset ?? 0n;
      return isInvalidOffset(committedOffset) ? resolved : resolved - (committedOffset ?? 0n);
    };

    let sum = 0n;
    for (const topic of this.topics) {
      const resolvedTopicOffsets = this.resolvedOffsets[topic] ?? {};
      const committedTopicOffsets = committedOffsets[topic] ?? {};
      for (const partition of Object.keys(resolvedTopicOffsets)) {
        sum += subtractOffsets(resolvedTopicOffsets[partition], committedTopicOffsets[partition]);
      }
    }

    return sum;
  }

  async setDefaultOffset({ topic, partition }: TopicPartition): Promise<void> {
    const reset = resolveAutoOffsetReset(this.topicConfigurations[topic]);
    if (reset === 'none') {
      throw new KafkaNonRetriableError(
        `Offset reset policy is none; no committed offset for topic ${topic} partition ${partition}`,
      );
    }

    const defaultOffset = this.cluster.defaultOffset({ fromBeginning: reset === 'earliest' });
    const coordinator = await this.getCoordinator();

    await coordinator.offsetCommit({
      groupId: this.groupId,
      memberId: this.memberId,
      groupGenerationId: this.generationId,
      topics: [{ topic, partitions: [{ partition, offset: defaultOffset }] }],
    });

    this.clearOffsets({ topic, partition });
  }

  /**
   * Commit the given offset to the topic/partition. NO-OP if this consumer isn't assigned to it.
   * With `autoCommit: false`, only the local resolved offset is updated (seek-offset minus one,
   * so `resolveOffset`'s +1 lands on the requested offset).
   */
  async seek({ topic, partition, offset }: TopicPartitionOffset): Promise<void> {
    const assigned = this.memberAssignment[topic];
    if (!assigned || !assigned.includes(partition)) {
      return;
    }

    if (!this.autoCommit) {
      this.resolveOffset({ topic, partition, offset: offset - 1n });
      return;
    }

    const coordinator = await this.getCoordinator();
    await coordinator.offsetCommit({
      groupId: this.groupId,
      memberId: this.memberId,
      groupGenerationId: this.generationId,
      topics: [{ topic, partitions: [{ partition, offset }] }],
    });

    this.clearOffsets({ topic, partition });
  }

  async commitOffsetsIfNecessary(): Promise<void> {
    if (this.autoCommitInterval == null && this.autoCommitThreshold == null) {
      await this.commitOffsets();
      return;
    }

    const now = Date.now();
    const timeoutReached = this.autoCommitInterval != null && now >= this.lastCommit + this.autoCommitInterval;
    const thresholdReached =
      this.autoCommitThreshold != null && this.countResolvedOffsets() >= BigInt(this.autoCommitThreshold);

    if (timeoutReached || thresholdReached) {
      await this.commitOffsets();
    }
  }

  uncommittedOffsets(): OffsetsByTopicPartition {
    const committedOffsets = this.committedOffsets();

    const topicsWithPartitionsToCommit = this.topics
      .map((topic) => {
        const resolvedTopic = this.resolvedOffsets[topic] ?? {};
        const committedTopic = committedOffsets[topic] ?? {};
        const partitions = Object.keys(resolvedTopic)
          .map((partition) => ({
            partition: Number(partition),
            offset: resolvedTopic[partition] ?? 0n,
          }))
          .filter(({ partition, offset }) => offset !== committedTopic[partition] && offset >= 0n);

        return { topic, partitions };
      })
      .filter(({ partitions }) => partitions.length > 0);

    return { topics: topicsWithPartitionsToCommit };
  }

  async commitOffsets(offsets: Offsets = {} as Offsets): Promise<void> {
    const topics = offsets.topics ?? this.uncommittedOffsets().topics;

    if (topics.length === 0) {
      this.lastCommit = Date.now();
      return;
    }

    const payload = {
      groupId: this.groupId,
      memberId: this.memberId,
      groupGenerationId: this.generationId,
      topics,
    };

    try {
      const coordinator = await this.getCoordinator();
      await coordinator.offsetCommit(payload);
      this.instrumentationEmitter.emit(COMMIT_OFFSETS, payload);

      for (const { topic, partitions } of topics) {
        const committedTopic = (this.committedOffsets()[topic] ??= {});
        for (const { partition, offset } of partitions) {
          committedTopic[partition] = offset;
        }
      }

      this.lastCommit = Date.now();
      if (this.hooks?.onCommit?.length) {
        await runHooks(this.hooks.onCommit, payload, 'onCommit', this.logger);
      }
    } catch (e) {
      const error = e as { type?: string };
      if (error.type === 'NOT_COORDINATOR_FOR_GROUP') {
        await this.cluster.refreshMetadata();
      }

      if (this.hooks?.onCommit?.length) {
        await runHooks(this.hooks.onCommit, { ...payload, error: e }, 'onCommit', this.logger);
      }
      throw e;
    }
  }

  async resolveOffsets(): Promise<void> {
    const pendingPartitions = this.topics
      .map((topic) => ({
        topic,
        partitions: (this.memberAssignment[topic] ?? [])
          .filter((partition) => isInvalidOffset(this.committedOffsets()[topic]?.[partition]))
          .map((partition) => ({ partition })),
      }))
      .filter((t) => t.partitions.length > 0);

    if (pendingPartitions.length === 0) {
      return;
    }

    const coordinator = await this.getCoordinator();
    const { responses: consumerOffsets } = await coordinator.offsetFetch({
      groupId: this.groupId,
      topics: pendingPartitions,
    });

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
        .filter((t) => t.partitions.length > 0 && resetByTopic[t.topic] !== 'none')
        .map((t) => ({
          topic: t.topic,
          partitions: t.partitions,
          fromBeginning: resetByTopic[t.topic] === 'earliest',
        }));

      const topicOffsets =
        topicsForListOffsets.length > 0 ? await this.cluster.fetchTopicsOffset(topicsForListOffsets) : [];
      offsets = initializeConsumerOffsets(consumerOffsets, topicOffsets, resetByTopic);
    }

    for (const { topic, partitions } of offsets) {
      const committedTopic = (this.committedOffsets()[topic] ??= {});
      for (const { partition, offset } of partitions) {
        committedTopic[partition] = offset;
      }
    }
  }

  clearOffsets({ topic, partition }: TopicPartition): void {
    const committed = this.committedOffsets()[topic];
    if (committed) delete committed[partition];
    const resolved = this.resolvedOffsets[topic];
    if (resolved) delete resolved[partition];
  }

  clearAllOffsets(): void {
    const committedOffsets = this.committedOffsets();

    for (const topic of Object.keys(committedOffsets)) {
      delete committedOffsets[topic];
    }

    for (const topic of this.topics) {
      committedOffsets[topic] = {};
    }

    this.resolvedOffsets = indexTopics(this.topics);
  }

  committedOffsets(): Record<string, Record<string, bigint>> {
    if (!this.#committedOffsets) {
      this.#committedOffsets = this.groupId ? this.cluster.committedOffsets({ groupId: this.groupId }) : {};
    }

    return this.#committedOffsets;
  }
}
