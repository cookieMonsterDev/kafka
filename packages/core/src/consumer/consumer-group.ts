import { randomUUID } from 'node:crypto';
import type { Broker } from '../broker/index';
import type { Cluster } from '../cluster/index';
import { KafkaError, KafkaNonRetriableError, KafkaStaleTopicMetadataAssignment, isRebalancing } from '../errors';
import type { InstrumentationEventEmitter } from '../instrumentation/emitter';
import type { Logger } from '../loggers/index';
import type { IsolationLevel } from '../protocol/enums/isolation-level';
import {
  CONSUMER_GROUP_JOIN_EPOCH,
  CONSUMER_GROUP_LEAVE_EPOCH,
} from '../protocol/requests/consumer-group-heartbeat/index';
import type { ConsumerGroupHeartbeatTopicPartitions } from '../protocol/requests/consumer-group-heartbeat/index';
import type { ConsumerGroupHeartbeatResponseV1Body } from '../protocol/requests/consumer-group-heartbeat/v1/response';
import { retrier, type RetryOptions } from '../retry/index';
import { arrayDiff } from '../utils/array-diff';
import { sharedPromiseTo } from '../utils/shared-promise-to';
import { sleep } from '../utils/wait';
import { MemberAssignment } from './assigner-protocol';
import { Batch } from './batch';
import { FetchSessionHandler } from './fetch-session';
import { CONNECT, GROUP_JOIN, HEARTBEAT, RECEIVED_UNSUBSCRIBED_TOPICS } from './instrumentation-events';
import { OffsetManager } from './offset-manager/index';
import type { TopicOffsetConfiguration } from './offset-reset';
import { SeekOffsets } from './seek-offsets';
import { SubscriptionState } from './subscription-state';
import type {
  Assigner,
  MemberAssignment as MemberAssignmentMap,
  Offsets,
  OffsetsByTopicPartition,
  RebalanceListener,
  TopicPartition,
  TopicPartitionOffset,
  TopicPartitions,
} from './types';

const STALE_METADATA_ERRORS = Object.freeze([
  'LEADER_NOT_AVAILABLE',
  'NOT_LEADER_FOR_PARTITION',
  'NOT_LEADER_OR_FOLLOWER',
  'FENCED_LEADER_EPOCH',
  'UNKNOWN_LEADER_EPOCH',
  'UNKNOWN_TOPIC_OR_PARTITION',
  'UNKNOWN_TOPIC_ID',
  'INCONSISTENT_TOPIC_ID',
  'REBOOTSTRAP_REQUIRED',
]);

const CONSUMER_REPLICA_ID = -1;

/** Sleep when a node has no fetchable partitions, instead of blocking for `maxWaitTimeInMs`. */
export const EMPTY_NODE_FETCH_SLEEP_MS = 100;

const ADAPTIVE_FULL_RATIO = 0.9;
const ADAPTIVE_SPARSE_RATIO = 0.25;
const ADAPTIVE_GROW_FACTOR = 1.5;
const ADAPTIVE_SHRINK_FACTOR = 0.5;

/**
 * Next Fetch `maxBytes` from the previous response fill ratio.
 *
 * Formula (clamped to `[min, max]`):
 * - `used/current >= 0.90` → grow: `floor(current * 1.5)`
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
  const lower = Math.max(1, min);
  const upper = Math.max(lower, max);
  const safeCurrent = Math.min(upper, Math.max(lower, current));
  if (used <= 0 || used / safeCurrent <= ADAPTIVE_SPARSE_RATIO) {
    return Math.max(lower, Math.floor(safeCurrent * ADAPTIVE_SHRINK_FACTOR));
  }
  if (used / safeCurrent >= ADAPTIVE_FULL_RATIO) {
    return Math.min(upper, Math.floor(safeCurrent * ADAPTIVE_GROW_FACTOR));
  }
  return safeCurrent;
}

/**
 * Sums each message's on-wire `byteSize` rather than its decoded `key`/`value` lengths — the
 * latter would force every message's `value`/`headers` to decode on every fetch (they're lazy;
 * see `DecodedRecord`), defeating that laziness for every `eachBatch`/`eachMessage` consumer.
 */
function estimateFetchedBytes(batches: readonly Batch[]): number {
  let bytes = 0;
  for (const batch of batches) {
    for (const message of batch.rawMessages) {
      bytes += message.byteSize;
    }
  }
  return bytes;
}

interface PreferredReadReplica {
  nodeId: number;
  expireAt: number;
}

function revokedPartitions(
  previousAssignment: readonly TopicPartitions[],
  nextAssignment: readonly TopicPartitions[],
): TopicPartitions[] {
  const nextPartitionsByTopic = new Map(nextAssignment.map(({ topic, partitions }) => [topic, new Set(partitions)]));

  return previousAssignment
    .map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.filter((partition) => !nextPartitionsByTopic.get(topic)?.has(partition)),
    }))
    .filter(({ partitions }) => partitions.length > 0);
}

/** `{ topic, partitions: number[] }[]` -> `{ topic, partition }[]`, for the rebalance callbacks. */
function flattenTopicPartitions(topicPartitions: readonly TopicPartitions[]): TopicPartition[] {
  return topicPartitions.flatMap(({ topic, partitions }) => partitions.map((partition) => ({ topic, partition })));
}

export interface ConsumerGroupOptions {
  retry?: RetryOptions;
  cluster: Cluster;
  groupId: string;
  topics: readonly string[];
  topicConfigurations: Record<string, TopicOffsetConfiguration>;
  logger: Logger;
  instrumentationEmitter: InstrumentationEventEmitter;
  assigners: readonly Assigner[];
  sessionTimeout: number;
  rebalanceTimeout: number;
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
  /** See {@link ConsumerRunConfig.onPartitionsRevoked}. */
  onPartitionsRevoked?: RebalanceListener;
  /** See {@link ConsumerRunConfig.onPartitionsAssigned}. */
  onPartitionsAssigned?: RebalanceListener;
  /** See {@link ConsumerRunConfig.onPartitionsLost}. */
  onPartitionsLost?: RebalanceListener;
}

/** Property-function shape so tests can fake/spy on these without unbound-method lint. */
export interface ConsumerGroupHandle {
  groupId: string;
  memberId: string | null;
  shuttingDown?: boolean;
  connect: () => Promise<void>;
  joinAndSync: () => Promise<void>;
  leave: () => Promise<void>;
  fetch: (nodeId: string) => Promise<Batch[]>;
  getNodeIds: () => string[];
  resolveOffset: (topicPartitionOffset: TopicPartitionOffset) => void;
  commitOffsets: (offsets?: Offsets) => Promise<void>;
  commitOffsetsIfNecessary: () => Promise<void>;
  uncommittedOffsets: () => OffsetsByTopicPartition;
  heartbeat: (options: { interval: number }) => Promise<void>;
  heartbeatDue: (interval: number) => boolean;
  pause: (topicPartitions: readonly { topic: string; partitions?: number[] }[]) => void;
  resume: (topicPartitions: readonly { topic: string; partitions?: number[] }[]) => void;
  isPaused: (topic: string, partition: number) => boolean;
  hasSeekOffset: (topicPartition: TopicPartition) => boolean;
  /**
   * Declares the member's current assignment lost without a clean revoke (session expiry,
   * fencing) and fires `onPartitionsLost` with it instead of `onPartitionsRevoked`. Callers
   * invoke this as soon as they detect `UNKNOWN_MEMBER_ID` / `FENCED_MEMBER_EPOCH`, before
   * rejoining, so a subsequent rebalance doesn't also report the same partitions as revoked.
   */
  notifyPartitionsLost: () => Promise<void>;
}

export class ConsumerGroup implements ConsumerGroupHandle {
  cluster: Cluster;
  groupId: string;
  topics: string[];
  topicsSubscribed: string[];
  topicConfigurations: Record<string, TopicOffsetConfiguration>;
  logger: Logger;
  instrumentationEmitter: InstrumentationEventEmitter;
  retrier: ReturnType<typeof retrier>;
  assigners: readonly Assigner[];
  sessionTimeout: number;
  rebalanceTimeout: number;
  maxBytesPerPartition: number;
  minBytes: number;
  maxBytes: number;
  maxWaitTime: number;
  autoCommit: boolean;
  autoCommitInterval: number | null;
  autoCommitThreshold: number | null;
  isolationLevel: IsolationLevel;
  rackId: string;
  metadataMaxAge: number;
  groupInstanceId: string | null;
  /** When true, membership uses ConsumerGroupHeartbeat (KIP-848) instead of JoinGroup/SyncGroup. */
  useConsumerProtocol: boolean;
  onPartitionsRevoked: RebalanceListener | undefined;
  onPartitionsAssigned: RebalanceListener | undefined;
  onPartitionsLost: RebalanceListener | undefined;
  shuttingDown = false;

  seekOffset = new SeekOffsets();
  coordinator: Broker | null = null;
  generationId: number | null = null;
  leaderId: string | null = null;
  memberId: string | null = null;
  members: { memberId: string; memberMetadata: Buffer; groupInstanceId?: string | null }[] | null = null;
  groupProtocol: string | null = null;
  partitionsPerSubscribedTopic: Map<string, number[]> | null = null;
  preferredReadReplicasPerTopicPartition: Record<string, Record<number, PreferredReadReplica>> = {};
  offsetManager: OffsetManager | null = null;
  subscriptionState = new SubscriptionState();
  lastRequest = Date.now();
  heartbeatIntervalMs: number | null = null;

  readonly #sharedHeartbeat: (options: { interval: number }) => Promise<void>;
  #includeJoinFields = true;
  #ownedTopicPartitions: ConsumerGroupHeartbeatTopicPartitions[] = [];
  #ownedPartitionsDirty = false;
  #consumerProtocolJoined = false;
  readonly #topicNameById = new Map<string, string>();
  #activeTopicPartitions: Record<string, Set<number>> | null = null;
  #adaptiveMaxBytes: number;
  /** KIP-320: the leader epoch each assigned partition was last fetched under, for truncation detection. */
  #lastFetchedLeaderEpoch: Record<string, Record<number, number>> = {};
  /** KIP-227: one incremental fetch session per broker node. */
  #fetchSessionHandlers = new Map<string, FetchSessionHandler>();

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
    maxBytesPerPartition,
    minBytes,
    maxBytes,
    maxWaitTimeInMs,
    autoCommit,
    autoCommitInterval,
    autoCommitThreshold,
    isolationLevel,
    rackId,
    metadataMaxAge,
    groupInstanceId,
    groupProtocol = 'classic',
    onPartitionsRevoked,
    onPartitionsAssigned,
    onPartitionsLost,
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
    this.sessionTimeout = sessionTimeout;
    this.rebalanceTimeout = rebalanceTimeout;
    this.maxBytesPerPartition = maxBytesPerPartition;
    this.minBytes = minBytes;
    this.maxBytes = maxBytes;
    this.maxWaitTime = maxWaitTimeInMs;
    this.autoCommit = autoCommit;
    this.autoCommitInterval = autoCommitInterval;
    this.autoCommitThreshold = autoCommitThreshold;
    this.isolationLevel = isolationLevel;
    this.rackId = rackId;
    this.metadataMaxAge = metadataMaxAge;
    this.groupInstanceId = groupInstanceId ?? null;
    this.useConsumerProtocol = groupProtocol === 'consumer';
    this.onPartitionsRevoked = onPartitionsRevoked;
    this.onPartitionsAssigned = onPartitionsAssigned;
    this.onPartitionsLost = onPartitionsLost;
    this.#adaptiveMaxBytes = maxBytes;

    this.#sharedHeartbeat = sharedPromiseTo(async ({ interval }: { interval: number }) => {
      if (this.useConsumerProtocol) {
        await this.#heartbeatConsumerProtocol({ interval });
        return;
      }

      const { groupId: id, generationId, memberId } = this;
      const now = Date.now();

      if (memberId && generationId != null && this.coordinator && now >= this.lastRequest + interval) {
        const payload = {
          groupId: id,
          memberId,
          groupGenerationId: generationId,
          groupInstanceId: this.groupInstanceId,
        };
        await this.coordinator.heartbeat(payload);
        this.instrumentationEmitter.emit(HEARTBEAT, payload);
        this.lastRequest = Date.now();
      }
    });
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

  async #join(): Promise<void> {
    const { groupId, sessionTimeout, rebalanceTimeout } = this;
    this.coordinator = await this.cluster.findGroupCoordinator({ groupId });

    const groupData = await this.coordinator.joinGroup({
      groupId,
      sessionTimeout,
      rebalanceTimeout,
      memberId: this.memberId ?? '',
      groupInstanceId: this.groupInstanceId,
      protocolType: 'consumer',
      groupProtocols: this.assigners.map((assigner) => assigner.protocol({ topics: this.topicsSubscribed })),
    });

    this.generationId = groupData.generationId;
    this.leaderId = groupData.leaderId;
    this.memberId = groupData.memberId;
    this.members = groupData.members;
    this.groupProtocol = groupData.groupProtocol;
  }

  async leave(): Promise<void> {
    await this.#closeFetchSessions();

    const { groupId, memberId, coordinator } = this;
    if (!memberId || !coordinator) return;

    if (this.useConsumerProtocol) {
      if (this.#consumerProtocolJoined) {
        await coordinator.consumerGroupHeartbeat({
          groupId,
          memberId,
          memberEpoch: CONSUMER_GROUP_LEAVE_EPOCH,
          instanceId: this.groupInstanceId,
        });
      }
      this.#consumerProtocolJoined = false;
      this.generationId = CONSUMER_GROUP_JOIN_EPOCH;
      this.#includeJoinFields = true;
      this.#ownedTopicPartitions = [];
      this.#ownedPartitionsDirty = false;
      return;
    }

    await coordinator.leaveGroup({ groupId, memberId, groupInstanceId: this.groupInstanceId });
    this.memberId = null;
  }

  async #sync(): Promise<boolean> {
    let assignment: { memberId: string; memberAssignment: Buffer }[] = [];
    const { groupId, generationId, memberId, members, groupProtocol, topicsSubscribed, coordinator } = this;

    if (!coordinator || generationId == null || memberId == null) {
      throw new KafkaNonRetriableError('Consumer group has not joined');
    }

    if (this.isLeader()) {
      this.logger.debug('Chosen as group leader', { groupId, generationId, memberId, topics: this.topics });
      const assigner = this.assigners.find(({ name }) => name === groupProtocol);

      if (!assigner) {
        throw new KafkaNonRetriableError(
          `Unsupported partition assigner "${groupProtocol}", the assigner wasn't found in the assigners list`,
        );
      }

      await this.cluster.refreshMetadata();
      assignment = await assigner.assign({ members: members ?? [], topics: topicsSubscribed });

      this.logger.debug('Group assignment', {
        groupId,
        generationId,
        groupProtocol,
        assignment,
        topics: topicsSubscribed,
      });
    }

    this.partitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic();
    const { memberAssignment } = await coordinator.syncGroup({
      groupId,
      generationId,
      memberId,
      groupInstanceId: this.groupInstanceId,
      protocolType: 'consumer',
      protocolName: groupProtocol,
      groupAssignment: assignment,
    });

    const decodedMemberAssignment = MemberAssignment.decode(memberAssignment);
    const decodedAssignment = decodedMemberAssignment != null ? decodedMemberAssignment.assignment : {};

    this.logger.debug('Received assignment', {
      groupId,
      generationId,
      memberId,
      memberAssignment: decodedAssignment,
    });

    const assignedTopics = Object.keys(decodedAssignment);
    const topicsNotSubscribed = arrayDiff(assignedTopics, topicsSubscribed);

    if (topicsNotSubscribed.length > 0) {
      const payload = {
        groupId,
        generationId,
        memberId,
        assignedTopics,
        topicsSubscribed,
        topicsNotSubscribed,
      };

      this.instrumentationEmitter.emit(RECEIVED_UNSUBSCRIBED_TOPICS, payload);
      this.logger.warn('Consumer group received unsubscribed topics', payload);
    }

    const safeAssignment = arrayDiff(assignedTopics, topicsNotSubscribed);
    const currentMemberAssignment = safeAssignment.map((topic) => ({
      topic,
      partitions: decodedAssignment[topic] ?? [],
    }));

    const previousAssignment = this.assigned();
    const selectedAssigner = this.assigners.find(({ name }) => name === groupProtocol);
    const isCooperative = selectedAssigner?.protocolType === 'cooperative';
    // Cooperative-sticky (KIP-429) settles over two JoinGroup/SyncGroup rounds: this round's
    // `currentMemberAssignment` already leaves foreign-owned partitions unassigned (see
    // `applyCooperativeConstraint` in sticky-assigner.ts), so `partitionsToRevoke` is only the
    // subset this member is actually giving up - not its whole previous assignment. A classic
    // (eager) rebalance has no such settling round: the member's entire prior assignment is
    // revoked and its entire new assignment is granted in one round.
    const partitionsToRevoke = isCooperative ? revokedPartitions(previousAssignment, currentMemberAssignment) : [];
    const revoked = isCooperative ? partitionsToRevoke : previousAssignment;
    const gained = isCooperative
      ? revokedPartitions(currentMemberAssignment, previousAssignment)
      : currentMemberAssignment;

    await this.#notify(this.onPartitionsRevoked, revoked, 'onPartitionsRevoked');

    await this.#installAssignment(currentMemberAssignment);

    selectedAssigner?.onAssignment?.(
      currentMemberAssignment.reduce<MemberAssignmentMap>(
        (partitionsByTopic, { topic, partitions }) => {
          partitionsByTopic[topic] = partitions;
          return partitionsByTopic;
        },
        Object.create(null) as MemberAssignmentMap,
      ),
    );

    await this.#notify(this.onPartitionsAssigned, gained, 'onPartitionsAssigned');

    if (partitionsToRevoke.length > 0) {
      this.logger.debug('Cooperative assignment revoked partitions; rejoining to settle assignment', {
        groupId,
        generationId,
        memberId,
        partitionsToRevoke,
      });
    }

    return partitionsToRevoke.length > 0;
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
   * with it (instead of `onPartitionsRevoked`). Callers must invoke this as soon as they detect
   * the member was fenced out of the group (`UNKNOWN_MEMBER_ID` / `FENCED_MEMBER_EPOCH`) -
   * before rejoining - both so the callback reflects reality (a lost partition may already be
   * owned by someone else; a pending offset commit for it should typically be abandoned) and so
   * the next successful rebalance's revoke/assign diff doesn't also report these same partitions
   * as revoked, since the local assignment is cleared here.
   */
  async notifyPartitionsLost(): Promise<void> {
    const lost = this.assigned();
    if (lost.length === 0) return;

    this.subscriptionState.assign([]);
    this.#invalidateActiveTopicPartitions();

    await this.#notify(this.onPartitionsLost, lost, 'onPartitionsLost');
  }

  async #installAssignment(currentMemberAssignment: TopicPartitions[]): Promise<void> {
    const { groupId, generationId, memberId, coordinator } = this;
    if (!coordinator || generationId == null || memberId == null) {
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
    this.subscriptionState.assign(currentMemberAssignment);
    this.#invalidateActiveTopicPartitions();

    this.offsetManager = new OffsetManager({
      cluster: this.cluster,
      topicConfigurations: this.topicConfigurations,
      instrumentationEmitter: this.instrumentationEmitter,
      memberAssignment: currentMemberAssignment.reduce<MemberAssignmentMap>(
        (partitionsByTopic, { topic, partitions }) => {
          partitionsByTopic[topic] = partitions;
          return partitionsByTopic;
        },
        {},
      ),
      autoCommit: this.autoCommit,
      autoCommitInterval: this.autoCommitInterval,
      autoCommitThreshold: this.autoCommitThreshold,
      coordinator,
      groupId,
      generationId,
      memberId,
    });
  }

  #ensureMemberId(): string {
    if (!this.memberId) {
      this.memberId = randomUUID();
    }
    return this.memberId;
  }

  #topicIdKey(topicId: Buffer): string {
    return topicId.toString('hex');
  }

  async #resolveTopicName(topicId: Buffer): Promise<string> {
    const key = this.#topicIdKey(topicId);
    const cached = this.#topicNameById.get(key);
    if (cached) return cached;

    if (this.topicsSubscribed.length === 1) {
      const name = this.topicsSubscribed[0];
      if (name != null) {
        this.#topicNameById.set(key, name);
        return name;
      }
    }

    await this.#refreshTopicIdMapFromDescribe();
    const resolved = this.#topicNameById.get(key);
    if (resolved) return resolved;

    throw new KafkaNonRetriableError(`Unable to resolve topic id ${key} to a subscribed topic name`);
  }

  async #refreshTopicIdMapFromDescribe(): Promise<void> {
    const { coordinator, groupId, memberId } = this;
    if (!coordinator) return;

    const { groups } = await coordinator.consumerGroupDescribe({
      groupIds: [groupId],
      includeAuthorizedOperations: false,
    });
    const group = groups.find((entry) => entry.groupId === groupId);
    if (!group) return;

    const members = memberId ? group.members.filter((member) => member.memberId === memberId) : group.members;
    for (const member of members) {
      for (const assignment of [member.assignment, member.targetAssignment]) {
        for (const { topicId, topicName } of assignment.topicPartitions) {
          if (topicName) this.#topicNameById.set(this.#topicIdKey(topicId), topicName);
        }
      }
    }
  }

  async #heartbeatConsumerProtocol({
    interval,
    force = false,
    ackDepth = 0,
  }: {
    interval: number;
    force?: boolean;
    ackDepth?: number;
  }): Promise<void> {
    const { groupId, coordinator } = this;
    if (!coordinator) {
      throw new KafkaNonRetriableError('Consumer group has not joined');
    }

    const memberId = this.#ensureMemberId();
    const heartbeatInterval = this.heartbeatIntervalMs ?? interval;
    const now = Date.now();
    if (!force && now < this.lastRequest + heartbeatInterval) return;

    const memberEpoch = this.generationId ?? CONSUMER_GROUP_JOIN_EPOCH;
    // Epoch 0 is a (re)join. The broker returns INVALID_REQUEST unless rebalanceTimeoutMs is
    // set, subscribedTopicNames (or regex) is non-null, and TopicPartitions is an empty list —
    // null means "unchanged", which is illegal before the member exists.
    const isJoining = memberEpoch === CONSUMER_GROUP_JOIN_EPOCH;
    const includeJoinFields = this.#includeJoinFields || isJoining;
    const includeOwnedPartitions = this.#ownedPartitionsDirty;
    const response = await coordinator.consumerGroupHeartbeat({
      groupId,
      memberId,
      memberEpoch,
      instanceId: includeJoinFields ? this.groupInstanceId : null,
      rackId: includeJoinFields ? (this.rackId === '' ? null : this.rackId) : null,
      rebalanceTimeoutMs: includeJoinFields ? this.rebalanceTimeout : -1,
      subscribedTopicNames: includeJoinFields ? [...this.topicsSubscribed] : null,
      topicPartitions: isJoining ? [] : includeOwnedPartitions ? this.#ownedTopicPartitions : null,
    });

    this.#includeJoinFields = false;
    this.#ownedPartitionsDirty = false;
    this.lastRequest = Date.now();
    this.instrumentationEmitter.emit(HEARTBEAT, {
      groupId,
      memberId,
      groupGenerationId: response.memberEpoch,
      groupInstanceId: this.groupInstanceId,
    });

    await this.#applyHeartbeatResponse(response);

    // Ack the new assignment on the next heartbeat immediately. The broker will not give
    // revoked partitions to other members until this member reports the updated owned set.
    if (this.#ownedPartitionsDirty && ackDepth < 8) {
      await this.#heartbeatConsumerProtocol({ interval: 0, force: true, ackDepth: ackDepth + 1 });
    }
  }

  async #applyHeartbeatResponse(response: ConsumerGroupHeartbeatResponseV1Body): Promise<void> {
    if (response.memberId) this.memberId = response.memberId;
    this.generationId = response.memberEpoch;
    this.heartbeatIntervalMs = response.heartbeatIntervalMs;
    this.leaderId = null;
    this.groupProtocol = 'consumer';
    const wasJoined = this.#consumerProtocolJoined;
    if (response.memberEpoch > CONSUMER_GROUP_JOIN_EPOCH) {
      this.#consumerProtocolJoined = true;
    }

    if (!response.assignment) return;

    const currentMemberAssignment: TopicPartitions[] = [];
    const owned: ConsumerGroupHeartbeatTopicPartitions[] = [];
    for (const { topicId, partitions } of response.assignment.topicPartitions) {
      const topic = await this.#resolveTopicName(topicId);
      currentMemberAssignment.push({ topic, partitions });
      owned.push({ topicId, partitions });
    }

    this.logger.debug('Received consumer-protocol assignment', {
      groupId: this.groupId,
      memberId: this.memberId,
      memberEpoch: this.generationId,
      memberAssignment: currentMemberAssignment,
    });

    // KIP-848 reconciliation is inherently incremental at the wire level - the broker sends this
    // member's full current target assignment on every change, and the client diffs it against
    // what it already held, same as a cooperative-sticky settling round above.
    const previousAssignment = this.assigned();
    const revoked = revokedPartitions(previousAssignment, currentMemberAssignment);
    const gained = revokedPartitions(currentMemberAssignment, previousAssignment);

    await this.#notify(this.onPartitionsRevoked, revoked, 'onPartitionsRevoked');

    await this.#installAssignment(currentMemberAssignment);
    this.#ownedTopicPartitions = owned;
    this.#ownedPartitionsDirty = true;

    await this.#notify(this.onPartitionsAssigned, gained, 'onPartitionsAssigned');

    if (wasJoined) this.#emitGroupJoin(0);
  }

  #emitGroupJoin(duration: number): void {
    const memberAssignment = this.assigned().reduce<MemberAssignmentMap>((result, { topic, partitions }) => {
      result[topic] = partitions;
      return result;
    }, {});
    this.instrumentationEmitter.emit(GROUP_JOIN, {
      groupId: this.groupId,
      memberId: this.memberId,
      leaderId: this.leaderId,
      isLeader: this.isLeader(),
      memberAssignment,
      groupProtocol: this.groupProtocol,
      duration,
    });
  }

  async #joinConsumerProtocol(): Promise<void> {
    this.coordinator = await this.cluster.findGroupCoordinator({ groupId: this.groupId });
    this.#includeJoinFields = true;
    this.#ownedPartitionsDirty = false;
    this.#ownedTopicPartitions = [];
    this.generationId = CONSUMER_GROUP_JOIN_EPOCH;
    this.#ensureMemberId();
    await this.cluster.refreshMetadata();

    await this.#heartbeatConsumerProtocol({ interval: 0, force: true });

    let attempts = 0;
    while (!this.#consumerProtocolJoined && !this.shuttingDown && attempts < 8) {
      attempts += 1;
      await this.#heartbeatConsumerProtocol({ interval: 0, force: true });
    }

    attempts = 0;
    while (this.#ownedPartitionsDirty && !this.shuttingDown && attempts < 8) {
      attempts += 1;
      await this.#heartbeatConsumerProtocol({ interval: 0, force: true });
    }
  }

  joinAndSync(): Promise<void> {
    const startJoin = Date.now();
    return this.retrier(async (bail) => {
      if (this.shuttingDown) return;

      try {
        if (this.useConsumerProtocol) {
          await this.#joinConsumerProtocol();
        } else {
          let requiresFollowupRebalance: boolean;
          do {
            await this.#join();
            if (this.shuttingDown) return;
            requiresFollowupRebalance = await this.#sync();
          } while (requiresFollowupRebalance && !this.shuttingDown);
        }

        if (this.shuttingDown) return;

        this.#emitGroupJoin(Date.now() - startJoin);
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

        if (error.type === 'UNKNOWN_MEMBER_ID' || error.type === 'FENCED_MEMBER_EPOCH') {
          // The coordinator no longer recognizes this member: it was fenced out of the group
          // (session timeout, or another member took its generation) before it ever got a chance
          // to leave and revoke cleanly. Whatever it held may already be reassigned elsewhere, so
          // report it lost - not revoked - before resetting and rejoining from scratch.
          await this.notifyPartitionsLost();
          if (error.type === 'UNKNOWN_MEMBER_ID') this.memberId = null;
          this.generationId = CONSUMER_GROUP_JOIN_EPOCH;
          this.#includeJoinFields = true;
          this.#consumerProtocolJoined = false;
          throw new KafkaError(error);
        }

        bail(error);
      }
    });
  }

  #requireOffsetManager(): OffsetManager {
    if (!this.offsetManager) {
      throw new KafkaNonRetriableError('Offset manager is not initialized');
    }
    return this.offsetManager;
  }

  resetOffset({ topic, partition }: TopicPartition): void {
    this.#requireOffsetManager().resetOffset({ topic, partition });
  }

  resolveOffset({ topic, partition, offset }: TopicPartitionOffset): void {
    this.#requireOffsetManager().resolveOffset({ topic, partition, offset });
  }

  seek({ topic, partition, offset }: TopicPartitionOffset): void {
    this.seekOffset.set(topic, partition, offset);
  }

  pause(topicPartitions: readonly { topic: string; partitions?: number[] }[]): void {
    this.logger.info(`Pausing fetching from ${topicPartitions.length} topics`, { topicPartitions });
    this.subscriptionState.pause(topicPartitions);
    this.#invalidateActiveTopicPartitions();
  }

  resume(topicPartitions: readonly { topic: string; partitions?: number[] }[]): void {
    this.logger.info(`Resuming fetching from ${topicPartitions.length} topics`, { topicPartitions });
    this.subscriptionState.resume(topicPartitions);
    this.#invalidateActiveTopicPartitions();
  }

  assigned(): TopicPartitions[] {
    return this.subscriptionState.assigned();
  }

  paused(): TopicPartitions[] {
    return this.subscriptionState.paused();
  }

  isPaused(topic: string, partition: number): boolean {
    return this.subscriptionState.isPaused(topic, partition);
  }

  async commitOffsetsIfNecessary(): Promise<void> {
    await this.#requireOffsetManager().commitOffsetsIfNecessary();
  }

  async commitOffsets(offsets?: Offsets): Promise<void> {
    await this.#requireOffsetManager().commitOffsets(offsets);
  }

  uncommittedOffsets(): OffsetsByTopicPartition {
    return this.#requireOffsetManager().uncommittedOffsets();
  }

  async heartbeat({ interval }: { interval: number }): Promise<void> {
    await this.#sharedHeartbeat({ interval });
  }

  heartbeatDue(interval: number): boolean {
    if (this.useConsumerProtocol) {
      if (!this.coordinator) return false;
      const heartbeatInterval = this.heartbeatIntervalMs ?? interval;
      return Date.now() >= this.lastRequest + heartbeatInterval;
    }

    return (
      this.memberId != null &&
      this.generationId != null &&
      this.coordinator != null &&
      Date.now() >= this.lastRequest + interval
    );
  }

  async fetch(nodeId: string): Promise<Batch[]> {
    try {
      await this.cluster.refreshMetadataIfNecessary();
      this.checkForStaleAssignment();

      let topicPartitions = this.subscriptionState.assigned();
      topicPartitions = this.filterPartitionsByNode(nodeId, topicPartitions);

      await this.seekOffsets(topicPartitions);

      const offsetManager = this.#requireOffsetManager();
      const committedOffsets = offsetManager.committedOffsets();
      const activeTopicPartitions = this.getActiveTopicPartitions();

      const requests = topicPartitions
        .map(({ topic, partitions }) => {
          const partitionMetadata = this.cluster.findTopicPartitionMetadata(topic);

          return {
            topic,
            topicId: this.cluster.findTopicId(topic),
            partitions: partitions
              .filter(
                (partition) =>
                  committedOffsets[topic]?.[partition] != null && activeTopicPartitions[topic]?.has(partition) === true,
              )
              .map((partition) => {
                const leaderEpoch = partitionMetadata.find((p) => p.partitionId === partition)?.leaderEpoch;
                if (leaderEpoch != null && leaderEpoch >= 0) {
                  (this.#lastFetchedLeaderEpoch[topic] ??= {})[partition] = leaderEpoch;
                }

                return {
                  partition,
                  currentLeaderEpoch: leaderEpoch,
                  fetchOffset: offsetManager.nextOffset(topic, partition),
                  maxBytes: this.maxBytesPerPartition,
                };
              }),
          };
        })
        .filter(({ partitions }) => partitions.length > 0);

      if (requests.length === 0) {
        await sleep(Math.min(EMPTY_NODE_FETCH_SLEEP_MS, this.maxWaitTime));
        return [];
      }

      const broker = await this.cluster.findBroker({ nodeId });
      const fetchSession = this.#fetchSessionHandlerFor(nodeId);
      const sessionRequest = fetchSession.buildRequest(requests);
      const { responses, sessionId } = await broker.fetch({
        replicaId: CONSUMER_REPLICA_ID,
        maxWaitTime: this.maxWaitTime,
        minBytes: this.minBytes,
        maxBytes: this.#adaptiveMaxBytes,
        isolationLevel: this.isolationLevel,
        topics: sessionRequest.topics,
        forgottenTopics: sessionRequest.forgottenTopics,
        sessionId: sessionRequest.sessionId,
        sessionEpoch: sessionRequest.sessionEpoch,
        topicsForResponse: requests,
        rackId: this.rackId,
      });
      fetchSession.handleResponse(sessionId ?? 0);

      const batches = responses.flatMap(({ topicName, partitions }) => {
        const topicRequestData = requests.find(({ topic }) => topic === topicName);

        let preferredReadReplicas = this.preferredReadReplicasPerTopicPartition[topicName];
        if (!preferredReadReplicas) {
          preferredReadReplicas = {};
          this.preferredReadReplicasPerTopicPartition[topicName] = preferredReadReplicas;
        }

        return partitions
          .filter(
            ({ partition }) =>
              !this.seekOffset.has(topicName, partition) && !this.subscriptionState.isPaused(topicName, partition),
          )
          .flatMap((partitionData) => {
            const { partition, preferredReadReplica } = partitionData;

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

            const partitionRequestData = topicRequestData?.partitions.find(
              ({ partition: id }) => id === partitionData.partition,
            );
            if (!partitionRequestData) return [];

            return [new Batch(topicName, partitionRequestData.fetchOffset, partitionData)];
          });
      });

      this.#adaptiveMaxBytes = nextAdaptiveMaxBytes({
        current: this.#adaptiveMaxBytes,
        used: estimateFetchedBytes(batches),
        min: this.minBytes,
        max: this.maxBytes,
      });

      return batches;
    } catch (e) {
      // The request as sent already advanced the session's local bookkeeping (buildRequest runs
      // before the RPC). If the RPC itself failed - a session error (FETCH_SESSION_ID_NOT_FOUND /
      // INVALID_FETCH_SESSION_EPOCH), or anything else - the broker's actual session state is no
      // longer known to match, so start over with a full fetch next time.
      this.#fetchSessionHandlerFor(nodeId).reset();
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
          await broker.fetch({
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
      type?: string;
      host?: string;
      port?: number;
      topic?: string;
      partition?: number;
      currentLeader?: { leaderId: number; leaderEpoch: number };
      nodeEndpoints?: { nodeId: number; host: string; port: number; rack: string | null }[];
      unknownPartitions?: unknown;
    };

    // KIP-227: the broker no longer recognizes this fetch session (evicted, or our epoch fell out
    // of sync). `fetch()` already reset the session handler for this node, so the next fetch is a
    // full one that opens a fresh session - nothing else to do.
    if (error.type === 'FETCH_SESSION_ID_NOT_FOUND' || error.type === 'INVALID_FETCH_SESSION_EPOCH') {
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
      STALE_METADATA_ERRORS.includes(error.type ?? '') &&
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

        if (error.type === 'FENCED_LEADER_EPOCH' || error.type === 'UNKNOWN_LEADER_EPOCH') {
          await this.recoverFromTruncation({ topic: error.topic, partition: error.partition });
        }

        return;
      }
    }

    if (STALE_METADATA_ERRORS.includes(error.type ?? '') || error.name === 'KafkaTopicMetadataNotLoaded') {
      this.logger.debug('Stale cluster metadata, refreshing...', {
        groupId: this.groupId,
        memberId: this.memberId,
        error: error.message,
      });

      await this.cluster.refreshMetadata();
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

    if (error.name === 'KafkaConnectionClosedError' && error.host != null && error.port != null) {
      this.cluster.removeBroker({ host: error.host, port: error.port });
      return;
    }

    if (error.name === 'KafkaBrokerNotFound' || error.name === 'KafkaConnectionClosedError') {
      this.logger.debug(`${error.message}, refreshing metadata and retrying...`);
      await this.cluster.refreshMetadata();
      return;
    }

    throw e;
  }

  async recoverFromOffsetOutOfRange(e: Error & { topic?: string; partition?: number }): Promise<void> {
    const preferredReadReplicas = e.topic ? this.preferredReadReplicasPerTopicPartition[e.topic] : undefined;
    // The stored value is `{ nodeId, expireAt }`, so `typeof ... === 'number'` is never true and
    // this always resets to the default offset. Preserved from the original implementation.
    if (preferredReadReplicas && e.partition != null && typeof preferredReadReplicas[e.partition] === 'number') {
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

      await this.#requireOffsetManager().setDefaultOffset({
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
      const { topics } = await broker.offsetForLeaderEpoch({
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
      const offsetManager = this.#requireOffsetManager();
      if (endOffset == null || endOffset < 0n || offsetManager.nextOffset(topic, partition) <= endOffset) {
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

      await offsetManager.seek({ topic, partition, offset: endOffset });
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
    if (!this.partitionsPerSubscribedTopic) return;

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

  async seekOffsets(topicPartitions: readonly TopicPartitions[]): Promise<void> {
    const offsetManager = this.#requireOffsetManager();

    for (const { topic, partitions } of topicPartitions) {
      for (const partition of partitions) {
        const seekEntry = this.seekOffset.pop(topic, partition);
        if (!seekEntry) continue;

        this.logger.debug('Seek offset', {
          groupId: this.groupId,
          memberId: this.memberId,
          seek: seekEntry,
        });
        await offsetManager.seek(seekEntry);
      }
    }

    await offsetManager.resolveOffsets();
  }

  hasSeekOffset({ topic, partition }: TopicPartition): boolean {
    return this.seekOffset.has(topic, partition);
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
          // Checks the leader (`nodeId`), not the preferred replica. Preserved from the original.
          if (Array.isArray(offlineReplicas) && offlineReplicas.includes(nodeId)) {
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

  getActiveTopicPartitions(): Record<string, Set<number>> {
    if (this.#activeTopicPartitions) return this.#activeTopicPartitions;

    const activeTopicPartitions: Record<string, Set<number>> = {};
    for (const { topic, partitions } of this.subscriptionState.active()) {
      activeTopicPartitions[topic] = new Set(partitions);
    }
    this.#activeTopicPartitions = activeTopicPartitions;
    return activeTopicPartitions;
  }

  #invalidateActiveTopicPartitions(): void {
    this.#activeTopicPartitions = null;
  }
}
