import type { Broker } from '../broker/index';
import type { Cluster } from '../cluster/index';
import { KafkaError, KafkaNonRetriableError, KafkaStaleTopicMetadataAssignment, isRebalancing } from '../errors';
import type { InstrumentationEventEmitter } from '../instrumentation/emitter';
import type { Logger } from '../loggers/index';
import type { IsolationLevel } from '../protocol/enums/isolation-level';
import { retrier, type RetryOptions } from '../retry/index';
import { arrayDiff } from '../utils/array-diff';
import { sharedPromiseTo } from '../utils/shared-promise-to';
import { sleep } from '../utils/wait';
import { MemberAssignment } from './assigner-protocol';
import { Batch } from './batch';
import { CONNECT, GROUP_JOIN, HEARTBEAT, RECEIVED_UNSUBSCRIBED_TOPICS } from './instrumentation-events';
import { OffsetManager } from './offset-manager/index';
import { SeekOffsets } from './seek-offsets';
import { SubscriptionState } from './subscription-state';
import type {
  Assigner,
  MemberAssignment as MemberAssignmentMap,
  Offsets,
  OffsetsByTopicPartition,
  TopicPartition,
  TopicPartitionOffset,
  TopicPartitions,
} from './types';

const STALE_METADATA_ERRORS = Object.freeze([
  'LEADER_NOT_AVAILABLE',
  'NOT_LEADER_FOR_PARTITION',
  'FENCED_LEADER_EPOCH',
  'UNKNOWN_LEADER_EPOCH',
  'UNKNOWN_TOPIC_OR_PARTITION',
]);

const CONSUMER_REPLICA_ID = -1;

interface PreferredReadReplica {
  nodeId: number;
  expireAt: number;
}

export interface ConsumerGroupOptions {
  retry?: RetryOptions;
  cluster: Cluster;
  groupId: string;
  topics: readonly string[];
  topicConfigurations: Record<string, { fromBeginning?: boolean }>;
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
  pause: (topicPartitions: readonly { topic: string; partitions?: number[] }[]) => void;
  resume: (topicPartitions: readonly { topic: string; partitions?: number[] }[]) => void;
  isPaused: (topic: string, partition: number) => boolean;
  hasSeekOffset: (topicPartition: TopicPartition) => boolean;
}

export class ConsumerGroup implements ConsumerGroupHandle {
  cluster: Cluster;
  groupId: string;
  topics: string[];
  topicsSubscribed: string[];
  topicConfigurations: Record<string, { fromBeginning?: boolean }>;
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

  readonly #sharedHeartbeat: (options: { interval: number }) => Promise<void>;

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

    this.#sharedHeartbeat = sharedPromiseTo(async ({ interval }: { interval: number }) => {
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
    const { groupId, memberId, coordinator } = this;
    if (memberId && coordinator) {
      await coordinator.leaveGroup({ groupId, memberId, groupInstanceId: this.groupInstanceId });
      this.memberId = null;
    }
  }

  async #sync(): Promise<void> {
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

  joinAndSync(): Promise<void> {
    const startJoin = Date.now();
    return this.retrier(async (bail) => {
      if (this.shuttingDown) return;

      try {
        await this.#join();
        if (this.shuttingDown) return;
        await this.#sync();

        const memberAssignment = this.assigned().reduce<MemberAssignmentMap>((result, { topic, partitions }) => {
          result[topic] = partitions;
          return result;
        }, {});

        const payload = {
          groupId: this.groupId,
          memberId: this.memberId,
          leaderId: this.leaderId,
          isLeader: this.isLeader(),
          memberAssignment,
          groupProtocol: this.groupProtocol,
          duration: Date.now() - startJoin,
        };

        this.instrumentationEmitter.emit(GROUP_JOIN, payload);
        this.logger.info('Consumer has joined the group', payload);
      } catch (e) {
        const error = e as Error & { type?: string };
        if (isRebalancing(error)) {
          // Rebalance in progress isn't a retriable protocol error: the consumer has to find the
          // coordinator and join again before it can retry. Wrapping in a retriable KafkaError
          // restarts the join + sync sequence through the retrier.
          throw new KafkaError(error);
        }

        if (error.type === 'UNKNOWN_MEMBER_ID') {
          this.memberId = null;
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
  }

  resume(topicPartitions: readonly { topic: string; partitions?: number[] }[]): void {
    this.logger.info(`Resuming fetching from ${topicPartitions.length} topics`, { topicPartitions });
    this.subscriptionState.resume(topicPartitions);
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
        .map(({ topic, partitions }) => ({
          topic,
          partitions: partitions
            .filter(
              (partition) =>
                committedOffsets[topic]?.[partition] != null && activeTopicPartitions[topic]?.has(partition) === true,
            )
            .map((partition) => ({
              partition,
              fetchOffset: offsetManager.nextOffset(topic, partition),
              maxBytes: this.maxBytesPerPartition,
            })),
        }))
        .filter(({ partitions }) => partitions.length > 0);

      if (requests.length === 0) {
        await sleep(this.maxWaitTime);
        return [];
      }

      const broker = await this.cluster.findBroker({ nodeId });
      const { responses } = await broker.fetch({
        replicaId: CONSUMER_REPLICA_ID,
        maxWaitTime: this.maxWaitTime,
        minBytes: this.minBytes,
        maxBytes: this.maxBytes,
        isolationLevel: this.isolationLevel,
        topics: requests,
        rackId: this.rackId,
      });

      return responses.flatMap(({ topicName, partitions }) => {
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
    } catch (e) {
      await this.recoverFromFetch(e);
      return [];
    }
  }

  async recoverFromFetch(e: unknown): Promise<void> {
    const error = e as Error & {
      type?: string;
      host?: string;
      port?: number;
      topic?: string;
      partition?: number;
      unknownPartitions?: unknown;
    };

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
    const activeTopicPartitions: Record<string, Set<number>> = {};
    for (const { topic, partitions } of this.subscriptionState.active()) {
      activeTopicPartitions[topic] = new Set(partitions);
    }
    return activeTopicPartitions;
  }
}
