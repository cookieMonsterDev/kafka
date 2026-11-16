import type { Broker } from '../broker/index';
import type { Cluster } from '../cluster/index';
import {
  isFetchSessionInvalid,
  isRebalancing,
  isRejectedMember,
  isStaleLeaderEpoch,
  isStaleMetadata,
  isUnknownMember,
} from '../cluster/error-classifier';
import { KafkaError, KafkaNonRetriableError, KafkaStaleTopicMetadataAssignment } from '../errors';
import type { InstrumentationEventEmitter } from '../instrumentation/emitter';
import type { Logger } from '../loggers/index';
import type { IsolationLevel } from '../protocol/enums/isolation-level';
import { retrier, type RetryOptions } from '../retry/index';
import { arrayDiff } from '../utils/array-diff';
import { sharedPromiseTo } from '../utils/shared-promise-to';
import { Batch } from './batch';
import { FetchSessionHandler } from './fetch-session';
import { CONNECT, GROUP_JOIN } from './instrumentation-events';
import { AssignModeMembership } from './membership/assign-mode-membership';
import { ClassicMembership } from './membership/classic-membership';
import { ConsumerProtocolMembership } from './membership/consumer-protocol-membership';
import {
  toPartitionsByTopic,
  type AssignmentChange,
  type GroupMembership,
  type MembershipHost,
} from './membership/group-membership';
import { AssignModeCommitTarget, GroupMemberCommitTarget } from './offset-manager/commit-target';
import { OffsetTracker } from './offset-manager/offset-tracker';
import type { TopicOffsetConfiguration } from './offset-reset';
import { PartitionPipeline } from './partition-pipeline';
import type {
  Assigner,
  ConsumerHooks,
  MemberAssignment as MemberAssignmentMap,
  Offsets,
  OffsetsByTopicPartition,
  RebalanceListener,
  TopicPartition,
  TopicPartitionOffset,
  TopicPartitions,
} from './types';
import { estimatePrefetchBytes } from './worker-queue';
import { Fetch } from '../protocol/requests/fetch/index';
import { OffsetForLeaderEpoch } from '../protocol/requests/offset-for-leader-epoch/index';

const CONSUMER_REPLICA_ID = -1;

/**
 * Sleep when a node has no fetchable partitions, or when a fetch sent without a long poll because
 * a partition is held back returns no records, instead of blocking for `maxWaitTimeInMs`.
 */
export const EMPTY_NODE_FETCH_SLEEP_MS = 100;

const ADAPTIVE_FULL_RATIO = 0.9;
const ADAPTIVE_SPARSE_RATIO = 0.25;
const ADAPTIVE_GROW_FACTOR = 1.5;
const ADAPTIVE_SHRINK_FACTOR = 0.5;

/**
 * Next Fetch `maxBytes` from the previous response fill ratio.
 *
 * Formula (clamped to `[min, max]`; `max` wins when `min > max`, and both are at least 1):
 * - `used/current >= 0.90` → grow: `ceil(current * 1.5)`
 * - `used/current <= 0.25` (including `used === 0`) → shrink: `floor(current * 0.5)`
 * - otherwise unchanged
 */
export function nextAdaptiveMaxBytes({
  current,
  used,
  min,
  max,
}: {
  current: number;
  used: number;
  min: number;
  max: number;
}): number {
  const upper = Math.max(1, max);
  const lower = Math.min(upper, Math.max(1, min));
  const safeCurrent = Math.min(upper, Math.max(lower, current));
  const fillRatio = used / safeCurrent;
  if (used <= 0 || fillRatio <= ADAPTIVE_SPARSE_RATIO) {
    return Math.max(lower, Math.floor(safeCurrent * ADAPTIVE_SHRINK_FACTOR));
  }
  if (fillRatio >= ADAPTIVE_FULL_RATIO) {
    return Math.min(upper, Math.ceil(safeCurrent * ADAPTIVE_GROW_FACTOR));
  }
  return safeCurrent;
}

function estimateFetchedBytes(batches: readonly Batch[]): number {
  let bytes = 0;
  for (const batch of batches) {
    bytes += estimatePrefetchBytes(batch);
  }
  return bytes;
}

interface PreferredReadReplica {
  nodeId: number;
  expireAt: number;
}

/** `{ topic, partitions: number[] }[]` -> `{ topic, partition }[]`, for the rebalance callbacks. */
function flattenTopicPartitions(topicPartitions: readonly TopicPartitions[]): TopicPartition[] {
  return topicPartitions.flatMap(({ topic, partitions }) => partitions.map((partition) => ({ topic, partition })));
}

export interface ConsumerGroupOptions {
  retry?: RetryOptions;
  cluster: Cluster;
  /** `null` when the consumer has no configured `groupId` (only possible in `assign()` mode). */
  groupId: string | null;
  topics: readonly string[];
  topicConfigurations: Record<string, TopicOffsetConfiguration>;
  logger: Logger;
  instrumentationEmitter: InstrumentationEventEmitter;
  assigners: readonly Assigner[];
  sessionTimeout: number;
  rebalanceTimeout: number;
  /** Heartbeat interval in milliseconds; under the consumer protocol, only until the broker sends its own. */
  heartbeatInterval: number;
  maxBytesPerPartition: number;
  minBytes: number;
  maxBytes: number;
  maxWaitTimeInMs: number;
  autoCommit: boolean;
  autoCommitInterval: number | null;
  autoCommitThreshold: number | null;
  isolationLevel: IsolationLevel;
  rackId: string;
  metadataMaxAge: number;
  groupInstanceId?: string;
  /**
   * Group membership protocol. `'classic'` uses JoinGroup/SyncGroup; `'consumer'` uses
   * ConsumerGroupHeartbeat (KIP-848). Default `'classic'`.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#group.protocol
   */
  groupProtocol?: 'classic' | 'consumer';
  /**
   * Server-side partition assignor to request under the KIP-848 consumer protocol
   * (`serverAssignor` on ConsumerGroupHeartbeat). Only meaningful when `groupProtocol` is
   * `'consumer'`; set with `groupProtocol: 'classic'` it is ignored (logged at debug level),
   * the same as this codebase's other consumer-protocol-only options.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#group.remote.assignor
   */
  groupRemoteAssignor?: 'uniform' | 'range';
  /**
   * Server-side regex subscription (KIP-848 `SubscriptionPattern`) sent as `subscribedTopicRegex`
   * on ConsumerGroupHeartbeat instead of a client-resolved topic list. Only meaningful when
   * `groupProtocol` is `'consumer'`; set with `groupProtocol: 'classic'` it is ignored (logged at
   * debug level). Kafka matches this pattern as RE2 server-side, which is not 1:1 with a JS
   * `RegExp` - callers should stick to RE2-compatible patterns.
   */
  subscribedTopicRegex?: string | null;
  /**
   * Offset-reset configuration applied to a topic the broker assigns through
   * `subscribedTopicRegex` (its name is unknown until the assignment arrives, so it cannot be
   * looked up in `topicConfigurations` by name ahead of time).
   */
  defaultTopicConfiguration?: TopicOffsetConfiguration;
  /** Ordered async `onCommit` hook, forwarded to this group's `OffsetTracker`. */
  hooks?: ConsumerHooks;
  /**
   * Verify each fetched record batch's CRC on decode; `false` skips the check for throughput.
   * Default `true`.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#check.crcs
   */
  checkCrcs?: boolean;
  /** See {@link ConsumerRunConfig.onPartitionsRevoked}. */
  onPartitionsRevoked?: RebalanceListener;
  /** See {@link ConsumerRunConfig.onPartitionsAssigned}. */
  onPartitionsAssigned?: RebalanceListener;
  /** See {@link ConsumerRunConfig.onPartitionsLost}. */
  onPartitionsLost?: RebalanceListener;
  /**
   * `consumer.assign()` mode: fetch exactly these partitions, skipping JoinGroup/SyncGroup,
   * ConsumerGroupHeartbeat, and rebalancing entirely.
   */
  assignedPartitions?: readonly TopicPartitions[];
}

/** Property-function shape so tests can fake/spy on these without unbound-method lint. */
export interface ConsumerGroupHandle {
  readonly groupId: string | null;
  readonly memberId: string | null;
  /**
   * When the next heartbeat is due, in epoch milliseconds, or `Infinity` when none is. Compared
   * per record, so it is a number rather than a method.
   */
  readonly nextHeartbeatAt: number;
  shuttingDown?: boolean;
  /** Each partition's fetch state: whether a batch is current, settling, and processing. */
  readonly partitionPipeline: PartitionPipeline;
  connect: () => Promise<void>;
  joinAndSync: () => Promise<void>;
  leave: () => Promise<void>;
  fetch: (nodeId: string) => Promise<Batch[]>;
  getNodeIds: () => string[];
  resolveOffset: (topicPartitionOffset: TopicPartitionOffset) => void;
  commitOffsets: (offsets?: Offsets) => Promise<void>;
  commitOffsetsIfNecessary: () => Promise<void>;
  uncommittedOffsets: () => OffsetsByTopicPartition;
  /** Sends a heartbeat when one is due. */
  heartbeat: () => Promise<void>;
  pause: (topicPartitions: readonly { topic: string; partitions?: number[] }[]) => void;
  resume: (topicPartitions: readonly { topic: string; partitions?: number[] }[]) => void;
  /**
   * Reports the current assignment lost and resets the member after `UNKNOWN_MEMBER_ID` /
   * `FENCED_MEMBER_EPOCH`. Callers invoke this as soon as they see either error, before rejoining.
   */
  recoverRejectedMember: (error: { type?: string }) => Promise<void>;
  /** Next fetch offset for `topic`/`partition`, or `null` when it isn't currently assigned. */
  position: (topic: string, partition: number) => bigint | null;
  /** Latest high watermark seen on a Fetch response for `topic`/`partition`, or `null` if none yet. */
  highWatermark: (topic: string, partition: number) => bigint | null;
}

export class ConsumerGroup implements ConsumerGroupHandle, MembershipHost {
  cluster: Cluster;
  groupId: string | null;
  topics: string[];
  topicsSubscribed: string[];
  topicConfigurations: Record<string, TopicOffsetConfiguration>;
  logger: Logger;
  instrumentationEmitter: InstrumentationEventEmitter;
  retrier: ReturnType<typeof retrier>;
  assigners: readonly Assigner[];
  maxBytesPerPartition: number;
  minBytes: number;
  maxBytes: number;
  maxWaitTime: number;
  isolationLevel: IsolationLevel;
  checkCrcs: boolean;
  rackId: string;
  metadataMaxAge: number;
  /** `serverAssignor` sent on ConsumerGroupHeartbeat; only set with the consumer group protocol. */
  readonly groupRemoteAssignor: 'uniform' | 'range' | null;
  /** `subscribedTopicRegex` sent on ConsumerGroupHeartbeat; only set with the consumer group protocol. */
  readonly subscribedTopicRegex: string | null;
  onPartitionsRevoked: RebalanceListener | undefined;
  onPartitionsAssigned: RebalanceListener | undefined;
  onPartitionsLost: RebalanceListener | undefined;
  shuttingDown = false;

  partitionsPerSubscribedTopic: Map<string, number[]> | null = null;
  preferredReadReplicasPerTopicPartition: Record<string, Record<number, PreferredReadReplica>> = {};
  /** The consumer's offsets for this run, told about each assignment change. */
  offsetTracker: OffsetTracker;
  readonly partitionPipeline = new PartitionPipeline({
    readPosition: (topic, partition) => this.offsetTracker.nextOffset(topic, partition),
  });

  readonly #membership: GroupMembership;
  readonly #sharedHeartbeat: () => Promise<void>;
  #adaptiveMaxBytes: number;
  /** KIP-320: the leader epoch each assigned partition was last fetched under, for truncation detection. */
  #lastFetchedLeaderEpoch: Record<string, Record<number, number>> = {};
  /** KIP-227: one incremental fetch session per broker node. */
  #fetchSessionHandlers = new Map<string, FetchSessionHandler>();
  /** Latest `highWatermark` seen on a Fetch response, per topic/partition. Backs {@link currentLag}. */
  #highWatermarks: Record<string, Record<number, bigint>> = {};

  constructor({
    retry,
    cluster,
    groupId,
    topics,
    topicConfigurations,
    logger,
    instrumentationEmitter,
    assigners,
    sessionTimeout,
    rebalanceTimeout,
    heartbeatInterval,
    maxBytesPerPartition,
    minBytes,
    maxBytes,
    maxWaitTimeInMs,
    autoCommit,
    autoCommitInterval,
    autoCommitThreshold,
    isolationLevel,
    checkCrcs = true,
    rackId,
    metadataMaxAge,
    groupInstanceId,
    groupProtocol = 'classic',
    groupRemoteAssignor,
    subscribedTopicRegex,
    defaultTopicConfiguration,
    hooks,
    onPartitionsRevoked,
    onPartitionsAssigned,
    onPartitionsLost,
    assignedPartitions,
  }: ConsumerGroupOptions) {
    this.cluster = cluster;
    this.groupId = groupId;
    this.topics = [...topics];
    this.topicsSubscribed = [...topics];
    this.topicConfigurations = topicConfigurations;
    this.logger = logger.namespace('ConsumerGroup');
    this.instrumentationEmitter = instrumentationEmitter;
    this.retrier = retrier({ ...retry });
    this.assigners = assigners;
    this.maxBytesPerPartition = maxBytesPerPartition;
    this.minBytes = minBytes;
    this.maxBytes = maxBytes;
    this.maxWaitTime = maxWaitTimeInMs;
    this.isolationLevel = isolationLevel;
    this.checkCrcs = checkCrcs;
    this.rackId = rackId;
    this.metadataMaxAge = metadataMaxAge;
    this.onPartitionsRevoked = onPartitionsRevoked;
    this.onPartitionsAssigned = onPartitionsAssigned;
    this.onPartitionsLost = onPartitionsLost;
    this.#adaptiveMaxBytes = maxBytes;

    const useConsumerProtocol = groupProtocol === 'consumer';
    if (groupRemoteAssignor != null && !useConsumerProtocol) {
      this.logger.debug('groupRemoteAssignor is only used with groupProtocol "consumer"; ignoring', {
        groupRemoteAssignor,
        groupProtocol,
      });
    }
    this.groupRemoteAssignor = useConsumerProtocol ? (groupRemoteAssignor ?? null) : null;

    if (subscribedTopicRegex != null && !useConsumerProtocol) {
      this.logger.debug('subscribedTopicRegex is only used with groupProtocol "consumer"; ignoring', {
        subscribedTopicRegex,
        groupProtocol,
      });
    }
    this.subscribedTopicRegex = useConsumerProtocol ? (subscribedTopicRegex ?? null) : null;

    if (assignedPartitions != null) {
      this.#membership = new AssignModeMembership(this, assignedPartitions);
    } else if (useConsumerProtocol) {
      this.#membership = new ConsumerProtocolMembership(this, {
        rebalanceTimeout,
        groupInstanceId: groupInstanceId ?? null,
        rackId,
        serverAssignor: this.groupRemoteAssignor,
        subscribedTopicRegex: this.subscribedTopicRegex,
        defaultTopicConfiguration: defaultTopicConfiguration ?? null,
        heartbeatInterval,
      });
    } else {
      this.#membership = new ClassicMembership(this, {
        sessionTimeout,
        rebalanceTimeout,
        groupInstanceId: groupInstanceId ?? null,
        heartbeatInterval,
      });
    }

    this.#sharedHeartbeat = sharedPromiseTo(() => this.#membership.heartbeat());

    // A group member without a group id never joins, so it has nowhere to commit either.
    const target =
      this.#membership.isGroupMember && groupId !== null
        ? new GroupMemberCommitTarget(cluster, groupId, this.#membership)
        : new AssignModeCommitTarget(cluster, groupId);
    this.offsetTracker = new OffsetTracker({
      cluster,
      target,
      topicConfigurations: this.topicConfigurations,
      instrumentationEmitter,
      autoCommit,
      // Assign mode commits only when asked to, so it has no auto-commit cadence.
      autoCommitInterval: target.isGroupMember ? autoCommitInterval : null,
      autoCommitThreshold: target.isGroupMember ? autoCommitThreshold : null,
      logger: this.logger,
      hooks,
    });
  }

  get coordinator(): Broker | null {
    return this.#membership.coordinator;
  }

  get memberId(): string | null {
    return this.#membership.memberId;
  }

  get generationId(): number | null {
    return this.#membership.generationId;
  }

  get leaderId(): string | null {
    return this.#membership.leaderId;
  }

  get groupProtocol(): string | null {
    return this.#membership.groupProtocol;
  }

  get nextHeartbeatAt(): number {
    return this.#membership.nextHeartbeatAt;
  }

  isLeader(): boolean {
    return this.leaderId != null && this.memberId === this.leaderId;
  }

  getNodeIds(): string[] {
    return this.cluster.getNodeIds();
  }

  async connect(): Promise<void> {
    await this.cluster.connect();
    this.instrumentationEmitter.emit(CONNECT, {});
    await this.cluster.refreshMetadataIfNecessary();
  }

  async leave(): Promise<void> {
    await this.#closeFetchSessions();
    await this.#membership.leave();
  }

  /**
   * Invokes a rebalance listener with the partitions moving in this step (skipped when none are
   * moving). Ordered async - the caller awaits this before proceeding - but errors are caught and
   * logged here rather than propagated: a broken user-supplied listener must not abort a
   * rebalance that would otherwise succeed, mirroring how this codebase treats other user-hook
   * failures (see the P3-04 `runHooks` convention for producer/consumer hooks).
   */
  async #notify(
    listener: RebalanceListener | undefined,
    topicPartitions: readonly TopicPartitions[],
    name: string,
  ): Promise<void> {
    if (!listener) return;
    const flat = flattenTopicPartitions(topicPartitions);
    if (flat.length === 0) return;

    try {
      await listener(flat);
    } catch (e) {
      const error = e as Error;
      this.logger.error(`Rebalance listener "${name}" threw an error`, { error: error.message, stack: error.stack });
    }
  }

  /**
   * Declares the current assignment lost without a clean revoke and fires `onPartitionsLost`
   * with it (instead of `onPartitionsRevoked`): a lost partition may already be owned by someone
   * else, so a pending offset commit for it should typically be abandoned. Clearing the local
   * assignment here also keeps the next rebalance's revoke/assign diff from reporting these same
   * partitions as revoked.
   */
  async notifyPartitionsLost(): Promise<void> {
    const lost = this.assigned();
    if (lost.length === 0) return;

    this.#updateAssignment([]);

    await this.#notify(this.onPartitionsLost, lost, 'onPartitionsLost');
  }

  /**
   * Recovers from `UNKNOWN_MEMBER_ID` (the session expired) or `FENCED_MEMBER_EPOCH` (the member
   * was fenced): the coordinator rejected the member before it could leave and revoke cleanly.
   * Reports the current assignment lost and resets the membership so the next
   * {@link joinAndSync} rejoins from scratch. Every path that sees either error calls this before
   * rejoining.
   */
  async recoverRejectedMember(error: { type?: string }): Promise<void> {
    await this.notifyPartitionsLost();
    this.#membership.resetRejectedMember({ forgetMemberId: isUnknownMember(error) });
  }

  async changeAssignment({ assignment, revoked, gained, onInstalled }: AssignmentChange): Promise<void> {
    await this.#notify(this.onPartitionsRevoked, revoked, 'onPartitionsRevoked');
    const partitionsByTopic = await this.#installAssignment(assignment);
    onInstalled?.(partitionsByTopic);
    await this.#notify(this.onPartitionsAssigned, gained, 'onPartitionsAssigned');
  }

  /** Installs the member's new assignment and returns it keyed by topic. */
  async #installAssignment(currentMemberAssignment: TopicPartitions[]): Promise<MemberAssignmentMap> {
    const { groupId, generationId, memberId, coordinator } = this;
    if (this.#membership.isGroupMember && (!coordinator || generationId == null || memberId == null)) {
      throw new KafkaNonRetriableError('Consumer group has not joined');
    }

    this.partitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic();

    for (const { topic, partitions: assignedPartitions } of currentMemberAssignment) {
      const knownPartitions = this.partitionsPerSubscribedTopic.get(topic) ?? [];
      const isAwareOfAllAssignedPartitions = assignedPartitions.every((partition) =>
        knownPartitions.includes(partition),
      );

      if (!isAwareOfAllAssignedPartitions) {
        this.logger.warn('Consumer is not aware of all assigned partitions, refreshing metadata', {
          groupId,
          generationId,
          memberId,
          topic,
          knownPartitions,
          assignedPartitions,
        });

        await this.cluster.refreshMetadata();
        this.partitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic();
        break;
      }
    }

    this.topics = currentMemberAssignment.map(({ topic }) => topic);
    return this.#updateAssignment(currentMemberAssignment);
  }

  /**
   * Tells the partition pipeline and the offset tracker about an assignment change. Every change
   * goes through here so the two keep the same assignment, and so the same retained partitions.
   */
  #updateAssignment(assignment: readonly TopicPartitions[]): MemberAssignmentMap {
    this.partitionPipeline.assign(assignment);
    const memberAssignment = toPartitionsByTopic(assignment);
    this.offsetTracker.updateAssignment(memberAssignment);
    return memberAssignment;
  }

  emitGroupJoin(durationMs: number): void {
    const memberAssignment = toPartitionsByTopic(this.assigned());
    this.instrumentationEmitter.emit(GROUP_JOIN, {
      groupId: this.groupId,
      memberId: this.memberId,
      leaderId: this.leaderId,
      isLeader: this.isLeader(),
      memberAssignment,
      groupProtocol: this.groupProtocol,
      duration: durationMs,
    });
  }

  joinAndSync(): Promise<void> {
    const startJoin = Date.now();
    return this.retrier(async (bail) => {
      if (this.shuttingDown) return;

      try {
        await this.#membership.join();
        if (this.shuttingDown || !this.#membership.isGroupMember) return;

        this.emitGroupJoin(Date.now() - startJoin);
        this.logger.info('Consumer has joined the group', {
          groupId: this.groupId,
          memberId: this.memberId,
          leaderId: this.leaderId,
          groupProtocol: this.groupProtocol,
          duration: Date.now() - startJoin,
        });
      } catch (e) {
        const error = e as Error & { type?: string };
        if (isRebalancing(error)) {
          // Rebalance in progress isn't a retriable protocol error: the consumer has to find the
          // coordinator and join again before it can retry. Wrapping in a retriable KafkaError
          // restarts the join + sync sequence through the retrier.
          throw new KafkaError(error);
        }

        if (isRejectedMember(error)) {
          await this.recoverRejectedMember(error);
          throw new KafkaError(error);
        }

        // Only short-circuit the retrier for genuinely non-retriable errors (e.g. auth
        // failures, invalid group state). Retriable protocol errors like
        // COORDINATOR_LOAD_IN_PROGRESS or NOT_COORDINATOR must fall through so the retrier can
        // retry them instead of failing the join immediately.
        if ((error as { retriable?: boolean }).retriable === false) {
          bail(error);
          return;
        }

        throw error;
      }
    });
  }

  resetOffset({ topic, partition }: TopicPartition): void {
    this.offsetTracker.resetOffset({ topic, partition });
    this.partitionPipeline.resetPrefetch(topic, partition);
  }

  resolveOffset({ topic, partition, offset }: TopicPartitionOffset): void {
    this.offsetTracker.resolveOffset({ topic, partition, offset });
  }

  seek({ topic, partition, offset }: TopicPartitionOffset): void {
    this.partitionPipeline.seek({ topic, partition, offset });
  }

  pause(topicPartitions: readonly { topic: string; partitions?: number[] }[]): void {
    this.logger.info(`Pausing fetching from ${topicPartitions.length} topics`, { topicPartitions });
    this.partitionPipeline.pause(topicPartitions);
  }

  resume(topicPartitions: readonly { topic: string; partitions?: number[] }[]): void {
    this.logger.info(`Resuming fetching from ${topicPartitions.length} topics`, { topicPartitions });
    this.partitionPipeline.resume(topicPartitions);
  }

  assigned(): TopicPartitions[] {
    return this.partitionPipeline.assigned();
  }

  paused(): TopicPartitions[] {
    return this.partitionPipeline.paused();
  }

  async commitOffsetsIfNecessary(): Promise<void> {
    await this.offsetTracker.commitOffsetsIfNecessary();
  }

  async commitOffsets(offsets?: Offsets): Promise<void> {
    await this.offsetTracker.commitOffsets(offsets);
  }

  uncommittedOffsets(): OffsetsByTopicPartition {
    return this.offsetTracker.uncommittedOffsets();
  }

  async heartbeat(): Promise<void> {
    await this.#sharedHeartbeat();
  }

  async fetch(nodeId: string): Promise<Batch[]> {
    try {
      await this.cluster.refreshMetadataIfNecessary();
      this.checkForStaleAssignment();

      const topicPartitions = this.filterPartitionsByNode(nodeId, this.partitionPipeline.assigned());

      await this.seekOffsets(topicPartitions);

      const { topics, hasHeldBackPartition } = this.partitionPipeline.planFetch(topicPartitions, (topic, partition) =>
        this.offsetTracker.hasCommittedOffset(topic, partition),
      );
      const requests = topics.map(({ topic, partitions }) => {
        const partitionMetadata = this.cluster.findTopicPartitionMetadata(topic);

        return {
          topic,
          topicId: this.cluster.findTopicId(topic),
          partitions: partitions.map(({ partition, fetchOffset }) => {
            const leaderEpoch = partitionMetadata.find((p) => p.partitionId === partition)?.leaderEpoch;
            if (leaderEpoch != null && leaderEpoch >= 0) {
              (this.#lastFetchedLeaderEpoch[topic] ??= {})[partition] = leaderEpoch;
            }

            return {
              partition,
              currentLeaderEpoch: leaderEpoch,
              fetchOffset,
              maxBytes: this.maxBytesPerPartition,
            };
          }),
        };
      });

      if (requests.length === 0) {
        await this.partitionPipeline.waitForFetchablePartition(Math.min(EMPTY_NODE_FETCH_SLEEP_MS, this.maxWaitTime));
        return [];
      }

      const wakeCount = this.partitionPipeline.wakeCount;
      const broker = await this.cluster.findBroker({ nodeId });
      const fetchSession = this.#fetchSessionHandlerFor(nodeId);
      const sessionRequest = fetchSession.buildRequest(requests);
      const response = await broker.send(Fetch, {
        replicaId: CONSUMER_REPLICA_ID,
        maxWaitTime: hasHeldBackPartition ? 0 : this.maxWaitTime,
        minBytes: this.minBytes,
        maxBytes: this.#adaptiveMaxBytes,
        isolationLevel: this.isolationLevel,
        checkCrcs: this.checkCrcs,
        topics: sessionRequest.topics,
        forgottenTopics: sessionRequest.forgottenTopics,
        sessionId: sessionRequest.sessionId,
        sessionEpoch: sessionRequest.sessionEpoch,
        topicsForResponse: requests,
        rackId: this.rackId,
      });
      // Fetch sessions (KIP-227) start at v7; an older response has no session id.
      fetchSession.handleResponse('sessionId' in response ? response.sessionId : 0);

      const batches = response.responses.flatMap(({ topicName, partitions }) => {
        const topicRequestData = requests.find(({ topic }) => topic === topicName);

        let preferredReadReplicas = this.preferredReadReplicasPerTopicPartition[topicName];
        if (!preferredReadReplicas) {
          preferredReadReplicas = {};
          this.preferredReadReplicasPerTopicPartition[topicName] = preferredReadReplicas;
        }

        return partitions.flatMap((partitionData) => {
          const { partition } = partitionData;
          // Fetching from a follower (KIP-392) starts at v11.
          const preferredReadReplica =
            'preferredReadReplica' in partitionData ? partitionData.preferredReadReplica : undefined;
          const partitionState = this.partitionPipeline.stateForFetchedPartition(topicName, partition);
          if (partitionState === null) return [];

          if (preferredReadReplica != null && preferredReadReplica !== -1) {
            const currentPreferredReadReplica = preferredReadReplicas[partition]?.nodeId;
            if (currentPreferredReadReplica !== preferredReadReplica) {
              this.logger.info(`Preferred read replica is now ${preferredReadReplica}`, {
                groupId: this.groupId,
                memberId: this.memberId,
                topic: topicName,
                partition,
              });
            }
            preferredReadReplicas[partition] = {
              nodeId: preferredReadReplica,
              expireAt: Date.now() + this.metadataMaxAge,
            };
          }

          const partitionRequestData = topicRequestData?.partitions.find(({ partition: id }) => id === partition);
          if (!partitionRequestData) return [];

          (this.#highWatermarks[topicName] ??= {})[partition] = partitionData.highWatermark;

          return [new Batch(topicName, partitionRequestData.fetchOffset, partitionData, partitionState)];
        });
      });

      for (const batch of batches) {
        this.partitionPipeline.track(batch);
      }

      // Floored at one partition's limit: below it the broker returns little more than the first
      // non-empty partition's first batch (KIP-74), so the other partitions wait a round trip each.
      this.#adaptiveMaxBytes = nextAdaptiveMaxBytes({
        current: this.#adaptiveMaxBytes,
        used: estimateFetchedBytes(batches),
        min: this.maxBytesPerPartition,
        max: this.maxBytes,
      });

      // Without the long poll, a response with no records would be refetched at once. Records
      // that are filtered out still move the position, and a wake while the fetch was in flight
      // means a held-back partition may be fetchable already, so both return without waiting.
      if (
        hasHeldBackPartition &&
        this.partitionPipeline.wakeCount === wakeCount &&
        batches.every((batch) => batch.rawMessages.length === 0)
      ) {
        await this.partitionPipeline.waitForFetchablePartition(Math.min(EMPTY_NODE_FETCH_SLEEP_MS, this.maxWaitTime));
      }

      return batches;
    } catch (e) {
      // The request as sent already advanced the session's local bookkeeping (buildRequest runs
      // before the RPC). If the RPC itself failed - a session error (FETCH_SESSION_ID_NOT_FOUND /
      // INVALID_FETCH_SESSION_EPOCH), or anything else - the broker's actual session state is no
      // longer known to match, so start over with a full fetch next time.
      this.#fetchSessionHandlerFor(nodeId).reset();
      // Recovery can move positions (offset reset, truncation); fetch from the consumed position.
      this.partitionPipeline.resetPrefetches();
      await this.recoverFromFetch(e);
      return [];
    }
  }

  #fetchSessionHandlerFor(nodeId: string): FetchSessionHandler {
    let handler = this.#fetchSessionHandlers.get(nodeId);
    if (!handler) {
      handler = new FetchSessionHandler();
      this.#fetchSessionHandlers.set(nodeId, handler);
    }
    return handler;
  }

  /** KIP-227: best-effort close of every open per-broker fetch session; the broker evicts idle sessions anyway. */
  async #closeFetchSessions(): Promise<void> {
    const handlers = [...this.#fetchSessionHandlers.entries()];
    this.#fetchSessionHandlers.clear();

    await Promise.all(
      handlers.map(async ([nodeId, handler]) => {
        const closeRequest = handler.closeRequest();
        if (!closeRequest) return;

        try {
          const broker = await this.cluster.findBroker({ nodeId });
          await broker.send(Fetch, {
            replicaId: CONSUMER_REPLICA_ID,
            maxWaitTime: 0,
            minBytes: 0,
            maxBytes: 0,
            isolationLevel: this.isolationLevel,
            topics: closeRequest.topics,
            forgottenTopics: closeRequest.forgottenTopics,
            sessionId: closeRequest.sessionId,
            sessionEpoch: closeRequest.sessionEpoch,
            rackId: this.rackId,
          });
        } catch (error) {
          this.logger.debug('Failed to close fetch session, the broker will evict it once idle', {
            groupId: this.groupId,
            memberId: this.memberId,
            nodeId,
            error: (error as Error).message,
          });
        }
      }),
    );
  }

  async recoverFromFetch(e: unknown): Promise<void> {
    const error = e as Error & {
      topic?: string;
      partition?: number;
      currentLeader?: { leaderId: number; leaderEpoch: number };
      nodeEndpoints?: { nodeId: number; host: string; port: number; rack: string | null }[];
      unknownPartitions?: unknown;
    };

    // KIP-227: the broker no longer recognizes this fetch session (evicted, or our epoch fell out
    // of sync). `fetch()` already reset the session handler for this node, so the next fetch is a
    // full one that opens a fresh session - nothing else to do.
    if (isFetchSessionInvalid(error)) {
      this.logger.debug('Fetch session invalidated by broker, retrying with a full fetch', {
        groupId: this.groupId,
        memberId: this.memberId,
        error: error.message,
      });
      return;
    }

    // KIP-951: the Fetch response itself named the new leader (and its address, if the client
    // didn't already have it cached). Patch the cache locally and retry - no Metadata RPC, no
    // group rejoin, since the assignment itself hasn't changed.
    if (
      isStaleMetadata(error) &&
      error.topic != null &&
      error.partition != null &&
      error.currentLeader != null &&
      error.currentLeader.leaderId >= 0
    ) {
      const patched = await this.cluster.applyLeaderUpdate({
        topic: error.topic,
        partition: error.partition,
        currentLeader: error.currentLeader,
        nodeEndpoints: error.nodeEndpoints ?? [],
      });

      if (patched) {
        this.logger.debug('Recovered leader from Fetch response, skipping metadata refresh', {
          groupId: this.groupId,
          memberId: this.memberId,
          topic: error.topic,
          partition: error.partition,
          leaderId: error.currentLeader.leaderId,
        });

        if (isStaleLeaderEpoch(error)) {
          await this.recoverFromTruncation({ topic: error.topic, partition: error.partition });
        }

        return;
      }
    }

    if (isStaleMetadata(error) || error.name === 'KafkaTopicMetadataNotLoaded') {
      this.logger.debug('Stale cluster metadata, refreshing...', {
        groupId: this.groupId,
        memberId: this.memberId,
        error: error.message,
      });

      await this.cluster.refreshStaleMetadata();
      await this.joinAndSync();
      return;
    }

    if (error.name === 'KafkaStaleTopicMetadataAssignment') {
      this.logger.warn(`${error.message}, resync group`, {
        groupId: this.groupId,
        memberId: this.memberId,
        topic: error.topic,
        unknownPartitions: error.unknownPartitions,
      });

      await this.joinAndSync();
      return;
    }

    if (error.name === 'KafkaOffsetOutOfRange') {
      if (
        error.topic != null &&
        error.partition != null &&
        (await this.recoverFromTruncation({ topic: error.topic, partition: error.partition }))
      ) {
        return;
      }

      await this.recoverFromOffsetOutOfRange(error);
      return;
    }

    if (this.cluster.onConnectionClosed(error)) {
      return;
    }

    if (error.name === 'KafkaBrokerNotFound' || error.name === 'KafkaConnectionClosedError') {
      this.logger.debug(`${error.message}, refreshing metadata and retrying...`);
      await this.cluster.refreshStaleMetadata();
      return;
    }

    throw e;
  }

  async recoverFromOffsetOutOfRange(e: Error & { topic?: string; partition?: number }): Promise<void> {
    const preferredReadReplicas = e.topic ? this.preferredReadReplicasPerTopicPartition[e.topic] : undefined;
    if (preferredReadReplicas && e.partition != null && preferredReadReplicas[e.partition] != null) {
      this.logger.info('Offset out of range while fetching from follower, retrying with leader', {
        topic: e.topic,
        partition: e.partition,
        groupId: this.groupId,
        memberId: this.memberId,
      });
      delete preferredReadReplicas[e.partition];
    } else if (e.topic != null && e.partition != null) {
      this.logger.error('Offset out of range, resetting to default offset', {
        topic: e.topic,
        partition: e.partition,
        groupId: this.groupId,
        memberId: this.memberId,
      });

      await this.offsetTracker.setDefaultOffset({
        topic: e.topic,
        partition: e.partition,
      });
    }
  }

  /**
   * KIP-320: after a leader change, ask the new leader for the end offset of the epoch this
   * consumer last fetched under. Seeks there only when the current fetch position is past it -
   * i.e. the broker's log was truncated out from under it - since a plain leader election
   * (no truncation) still resolves the epoch to an end offset at or beyond that position.
   * Returns `false` (the caller falls back to its own recovery) when there is no previously
   * fetched epoch to validate, the partition has no known leader, or the broker predates
   * OffsetForLeaderEpoch.
   */
  async recoverFromTruncation({ topic, partition }: { topic: string; partition: number }): Promise<boolean> {
    const lastFetchedEpoch = this.#lastFetchedLeaderEpoch[topic]?.[partition];
    if (lastFetchedEpoch == null || lastFetchedEpoch < 0) return false;

    const partitionMetadata = this.cluster.findTopicPartitionMetadata(topic).find((p) => p.partitionId === partition);
    if (!partitionMetadata || partitionMetadata.leader == null) return false;

    try {
      const broker = await this.cluster.findBroker({ nodeId: String(partitionMetadata.leader) });
      const { topics } = await broker.send(OffsetForLeaderEpoch, {
        topics: [
          {
            topic,
            partitions: [
              { partition, currentLeaderEpoch: partitionMetadata.leaderEpoch, leaderEpoch: lastFetchedEpoch },
            ],
          },
        ],
      });

      const endOffset = topics[0]?.partitions[0]?.endOffset;
      if (endOffset == null || endOffset < 0n || this.offsetTracker.nextOffset(topic, partition) <= endOffset) {
        return false;
      }

      this.logger.warn('Detected log truncation, seeking to the leader epoch end offset', {
        groupId: this.groupId,
        memberId: this.memberId,
        topic,
        partition,
        lastFetchedEpoch,
        endOffset,
      });

      await this.offsetTracker.seek({ topic, partition, offset: endOffset });
      delete this.#lastFetchedLeaderEpoch[topic]?.[partition];
      return true;
    } catch {
      return false;
    }
  }

  generatePartitionsPerSubscribedTopic(): Map<string, number[]> {
    const map = new Map<string, number[]>();

    for (const topic of this.topicsSubscribed) {
      const partitions = this.cluster
        .findTopicPartitionMetadata(topic)
        .map((m) => m.partitionId)
        .sort((a, b) => a - b);

      map.set(topic, partitions);
    }

    return map;
  }

  checkForStaleAssignment(): void {
    // assign() mode assignment is fixed by the caller; there is no rejoin to trigger, so a
    // partition-count change on the topic is not this consumer's concern to reconcile.
    if (!this.#membership.isGroupMember || !this.partitionsPerSubscribedTopic) return;

    const newPartitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic();

    for (const [topic, partitions] of newPartitionsPerSubscribedTopic) {
      const diff = arrayDiff(partitions, this.partitionsPerSubscribedTopic.get(topic) ?? []);

      if (diff.length > 0) {
        throw new KafkaStaleTopicMetadataAssignment('Topic has been updated', {
          topic,
          unknownPartitions: diff,
        });
      }
    }
  }

  /** Applies pending seeks of `topicPartitions`, except on partitions a worker is still handling. */
  async seekOffsets(topicPartitions: readonly TopicPartitions[]): Promise<void> {
    await this.partitionPipeline.applyPendingSeeks(topicPartitions, async (seek) => {
      this.logger.debug('Seek offset', {
        groupId: this.groupId,
        memberId: this.memberId,
        seek,
      });
      await this.offsetTracker.seek(seek);
    });

    await this.offsetTracker.resolveOffsets();
  }

  /**
   * Next fetch offset for `topic`/`partition` - the offset of the next record this consumer
   * would read. Returns `null` when the partition is not currently assigned to this consumer,
   * rather than throwing, since "not assigned" is an expected state (e.g. after a rebalance).
   */
  position(topic: string, partition: number): bigint | null {
    if (!this.partitionPipeline.isAssigned(topic, partition)) return null;

    return this.offsetTracker.nextOffset(topic, partition);
  }

  /**
   * Latest high watermark seen on a Fetch response for `topic`/`partition`. Returns `null` when
   * no Fetch response has been observed for it yet, rather than throwing.
   */
  highWatermark(topic: string, partition: number): bigint | null {
    return this.#highWatermarks[topic]?.[partition] ?? null;
  }

  /**
   * For each partition, pick the preferred read replica if one is recorded and still fresh,
   * otherwise the leader. Each partition appears in the result exactly once.
   */
  findReadReplicaForPartitions(topic: string, partitions: readonly number[]): Record<string, number[]> {
    const partitionMetadata = this.cluster.findTopicPartitionMetadata(topic);
    const preferredReadReplicas = this.preferredReadReplicasPerTopicPartition[topic];

    return partitions.reduce<Record<string, number[]>>((result, partitionId) => {
      const metadata = partitionMetadata.find((p) => p.partitionId === partitionId);
      if (!metadata) return result;

      if (metadata.leader == null) {
        throw new KafkaError('Invalid partition metadata', { cause: { topic, partitionId, metadata } });
      }

      let nodeId: number = metadata.leader;
      if (preferredReadReplicas) {
        const preferred = preferredReadReplicas[partitionId];
        const expireAt = preferred?.expireAt;
        const preferredReadReplica = preferred?.nodeId;

        if (expireAt != null && Date.now() >= expireAt) {
          this.logger.debug('Preferred read replica information has expired, using leader', {
            topic,
            partitionId,
            groupId: this.groupId,
            memberId: this.memberId,
            preferredReadReplica,
            leader: metadata.leader,
          });
          delete preferredReadReplicas[partitionId];
        } else if (preferredReadReplica != null) {
          const offlineReplicas = metadata.offlineReplicas;
          if (Array.isArray(offlineReplicas) && offlineReplicas.includes(preferredReadReplica)) {
            this.logger.debug('Preferred read replica is offline, using leader', {
              topic,
              partitionId,
              groupId: this.groupId,
              memberId: this.memberId,
              preferredReadReplica,
              leader: metadata.leader,
            });
          } else {
            nodeId = preferredReadReplica;
          }
        }
      }

      const key = String(nodeId);
      const current = result[key] ?? [];
      return { ...result, [key]: [...current, partitionId] };
    }, {});
  }

  filterPartitionsByNode(nodeId: string, topicPartitions: readonly TopicPartitions[]): TopicPartitions[] {
    return topicPartitions.map(({ topic, partitions }) => ({
      topic,
      partitions: this.findReadReplicaForPartitions(topic, partitions)[nodeId] ?? [],
    }));
  }
}
