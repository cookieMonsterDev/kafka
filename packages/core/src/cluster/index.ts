import { Broker } from '../broker/index';
import { EARLIEST_OFFSET, LATEST_OFFSET } from '../constants';
import {
  KafkaBrokerNotFound,
  KafkaError,
  KafkaGroupCoordinatorNotFound,
  KafkaMetadataNotLoaded,
  KafkaTopicMetadataNotLoaded,
} from '../errors';
import { ClientTelemetryReporter } from '../instrumentation/client-telemetry';
import type { InstrumentationEventEmitter } from '../instrumentation/emitter';
import type { ProduceMetrics } from '../instrumentation/metrics';
import { TelemetrySnapshot } from '../instrumentation/telemetry-snapshot';
import type { Logger } from '../loggers/index';
import type { ConnectionOptions } from '../network/connection';
import type { ClientDnsLookup } from '../network/dns-lookup';
import type { SocketFactory } from '../network/socket-factory';
import { COORDINATOR_TYPES } from '../protocol/enums/coordinator-types';
import type { CoordinatorType } from '../protocol/enums/coordinator-types';
import type { IsolationLevel } from '../protocol/enums/isolation-level';
import { ZERO_TOPIC_ID, type ClusterMetadata } from '../protocol/requests/metadata/shared';
import type { RetryOptions } from '../retry/index';
import { retrier } from '../retry/index';
import {
  closedConnectionAddress,
  isConnectionError,
  isConnectionRefused,
  isStaleCoordinator,
  isStaleMetadata,
  isTopicAuthorizationFailed,
  isUnknownTopic,
  RECOVERY_ACTIONS,
  recoveryAction,
} from './error-classifier';
import { runOnTargets, type BrokerTarget, type RunContext, type RunOptions, type Targets } from './run';
import { Lock } from '../utils/lock';
import { sharedPromiseTo } from '../utils/shared-promise-to';
import { BrokerPool, type ApplyLeaderUpdateOptions, type MetadataRecovery } from './broker-pool';
import { connectionPoolBuilder } from './connection-pool-builder';
import { FindCoordinator, type FindCoordinatorResponse } from '../protocol/requests/find-coordinator/index';
import { ListOffsets } from '../protocol/requests/list-offsets/index';
import { Metadata } from '../protocol/requests/metadata/index';

type MetadataBroker = ClusterMetadata['brokers'][number];
type MetadataTopic = ClusterMetadata['topicMetadata'][number];
type PartitionMetadata = MetadataTopic['partitionMetadata'][number];

export interface FetchTopicsOffsetPartition {
  partition: number;
}

export interface FetchTopicsOffsetTopic {
  topic: string;
  partitions: readonly FetchTopicsOffsetPartition[];
  fromBeginning?: boolean;
  fromTimestamp?: bigint;
}

export interface TopicOffsetPartition {
  partition: number;
  offset: bigint;
}

export interface TopicOffsets {
  topic: string;
  partitions: TopicOffsetPartition[];
}

/** Committed offsets by group id, then topic, then partition. */
export type CommittedOffsetsByGroup = Map<string, Map<string, Map<number, bigint>>>;

export interface ClusterOptions {
  logger: Logger;
  socketFactory: SocketFactory;
  brokers: readonly string[] | (() => readonly string[] | Promise<readonly string[]>);
  ssl?: ConnectionOptions['ssl'];
  sasl?: ConnectionOptions['sasl'];
  clientId: string;
  connectionTimeout: number;
  authenticationTimeout?: number;
  reauthenticationThreshold?: number;
  requestTimeout?: number;
  enforceRequestTimeout?: boolean;
  metadataMaxAge?: number;
  metadataRecovery?: MetadataRecovery;
  retry?: RetryOptions;
  allowAutoTopicCreation?: boolean;
  maxInFlightRequests?: number | null;
  isolationLevel?: IsolationLevel;
  instrumentationEmitter?: InstrumentationEventEmitter | null;
  offsets?: CommittedOffsetsByGroup;
  /**
   * KIP-919: bootstrap from controller endpoints via DescribeCluster instead of Metadata.
   * Admin-only; producer and consumer always discover brokers.
   */
  usingBootstrapControllers?: boolean;
  connectionsMaxIdleMs?: number;
  clientDnsLookup?: ClientDnsLookup;
  socketConnectionSetupTimeoutMaxMs?: number;
  reconnectBackoffMs?: number;
  reconnectBackoffMaxMs?: number;
  /**
   * KIP-714: push client metrics to the broker when it advertises GetTelemetrySubscriptions.
   * Default true. Brokers without API 71 disable the pusher automatically.
   */
  enableMetricsPush?: boolean;
}

/**
 * The topic-and-partition-aware layer: metadata refresh, leader lookup, group-coordinator lookup,
 * and target-topic tracking sit here, on top of `BrokerPool`. Concurrency-sensitive operations
 * (`connect`, metadata refresh, finding the controller broker) are wrapped in `sharedPromiseTo` so
 * concurrent callers share one in-flight attempt instead of racing duplicate work.
 */
export class Cluster {
  readonly rootLogger: Logger;
  readonly logger: Logger;
  readonly retrier: ReturnType<typeof retrier>;
  readonly brokerPool: BrokerPool;
  readonly isolationLevel: IsolationLevel | undefined;
  readonly allowAutoTopicCreation: boolean;

  targetTopics = new Set<string>();
  readonly mutatingTargetTopics: Lock;

  readonly #committedOffsetsByGroup: CommittedOffsetsByGroup;
  readonly #connect: () => Promise<void>;
  readonly #refreshMetadata: () => Promise<void>;
  readonly #refreshMetadataIfNecessary: () => Promise<void>;
  readonly #refreshStaleMetadata: () => Promise<void>;
  readonly #runContext: RunContext;
  readonly #findControllerBroker: () => Promise<Broker>;
  readonly #telemetry: ClientTelemetryReporter | null;

  constructor({
    logger: rootLogger,
    socketFactory,
    brokers,
    ssl,
    sasl,
    clientId,
    connectionTimeout,
    authenticationTimeout,
    reauthenticationThreshold,
    requestTimeout = 30_000,
    enforceRequestTimeout,
    metadataMaxAge,
    metadataRecovery,
    retry,
    allowAutoTopicCreation,
    maxInFlightRequests,
    isolationLevel,
    instrumentationEmitter = null,
    offsets = new Map(),
    usingBootstrapControllers = false,
    connectionsMaxIdleMs,
    clientDnsLookup,
    socketConnectionSetupTimeoutMaxMs,
    reconnectBackoffMs,
    reconnectBackoffMaxMs,
    enableMetricsPush = true,
  }: ClusterOptions) {
    this.rootLogger = rootLogger;
    this.logger = rootLogger.namespace('Cluster');
    this.retrier = retrier(retry);

    const builder = connectionPoolBuilder({
      logger: rootLogger,
      instrumentationEmitter,
      socketFactory,
      brokers,
      ssl,
      sasl,
      clientId,
      connectionTimeout,
      requestTimeout,
      enforceRequestTimeout,
      maxInFlightRequests,
      reauthenticationThreshold,
      connectionsMaxIdleMs,
      clientDnsLookup,
      socketConnectionSetupTimeoutMaxMs,
      reconnectBackoffMs,
      reconnectBackoffMaxMs,
    });

    this.targetTopics = new Set();
    this.mutatingTargetTopics = new Lock({ description: 'updating target topics', timeout: requestTimeout });
    this.isolationLevel = isolationLevel;
    this.allowAutoTopicCreation = allowAutoTopicCreation !== false;
    this.brokerPool = new BrokerPool({
      connectionPoolBuilder: builder,
      logger: this.rootLogger,
      retry,
      allowAutoTopicCreation,
      authenticationTimeout,
      metadataMaxAge,
      metadataRecovery,
      usingBootstrapControllers,
    });
    this.#committedOffsetsByGroup = offsets;

    const snapshot = new TelemetrySnapshot();
    if (instrumentationEmitter) snapshot.bind(instrumentationEmitter);
    this.#telemetry =
      enableMetricsPush === false
        ? null
        : new ClientTelemetryReporter({
            getBroker: () => this.brokerPool.findConnectedBroker(),
            logger: this.rootLogger,
            clientId,
            snapshot,
          });

    this.#connect = sharedPromiseTo(async () => {
      await this.brokerPool.connect();
      this.#telemetry?.start();
    });

    this.#refreshMetadata = sharedPromiseTo(() =>
      this.#withoutUnknownTargetTopics(() => this.brokerPool.refreshMetadata([...this.targetTopics])),
    );

    this.#refreshMetadataIfNecessary = sharedPromiseTo(() =>
      this.brokerPool.refreshMetadataIfNecessary([...this.targetTopics]),
    );

    // Goes through the update queue's staleness re-check, so callers recovering from the same
    // stale metadata share one Metadata request.
    this.#refreshStaleMetadata = sharedPromiseTo(() => {
      this.brokerPool.expireMetadata();
      return this.#withoutUnknownTargetTopics(() => this.brokerPool.refreshMetadataIfNecessary([...this.targetTopics]));
    });

    this.#runContext = {
      logger: this.logger,
      retrier: this.retrier,
      resolve: (target) => this.resolveTarget(target),
      refreshMetadata: () => this.refreshMetadata(),
      recover: (error) => this.recover(error),
    };

    this.#findControllerBroker = sharedPromiseTo(async () => {
      const { metadata } = this.brokerPool;

      if (!metadata || metadata.controllerId == null) {
        throw new KafkaMetadataNotLoaded('Topic metadata not loaded');
      }

      const broker = await this.findBroker({ nodeId: String(metadata.controllerId) });

      if (!broker) {
        throw new KafkaBrokerNotFound(
          `Controller broker with id ${metadata.controllerId} not found in the cached metadata`,
        );
      }

      return broker;
    });
  }

  isConnected(): boolean {
    return this.brokerPool.hasConnectedBrokers();
  }

  /**
   * Runs `refresh`, dropping a target topic the broker reports unknown and trying again, unless
   * this cluster creates topics on first use.
   */
  async #withoutUnknownTargetTopics(refresh: () => Promise<void>): Promise<void> {
    while (true) {
      try {
        await refresh();
        return;
      } catch (e) {
        const error = e as Error & { type?: string; topic?: string };
        if (
          error.topic &&
          this.targetTopics.has(error.topic) &&
          isUnknownTopic(error) &&
          !this.allowAutoTopicCreation
        ) {
          this.targetTopics.delete(error.topic);
          continue;
        }
        throw e;
      }
    }
  }

  /**
   * Resolves `target`, runs `work` on its broker and retries. Admin requests, InitProducerId and
   * TxnOffsetCommit use it; it suits other low-rate coordinator or controller requests too. Each
   * attempt resolves the target again.
   * An error is retried when the target moved (`NOT_CONTROLLER` for the controller, a moved or
   * unreachable coordinator, a stale partition leader) or its type is in `options.retryOn`, after
   * {@link recover}; any other error rejects as is. With a function, `targets` is read on every
   * attempt and `work` gets one broker per target.
   *
   * Produce and fetch keep their own retry loops: a retrier per request would cost a promise and
   * several closures each time, and would nest inside the producer's flush retries.
   */
  run<T>(target: BrokerTarget, work: (broker: Broker) => Promise<T>, options?: RunOptions): Promise<T>;
  run<T>(
    targets: () => readonly BrokerTarget[],
    work: (brokers: Broker[]) => Promise<T>,
    options?: RunOptions,
  ): Promise<T>;
  run<T>(
    targets: Targets,
    work: ((broker: Broker) => Promise<T>) | ((brokers: Broker[]) => Promise<T>),
    options?: RunOptions,
  ): Promise<T> {
    return runOnTargets(this.#runContext, targets, work, options);
  }

  /** The broker that currently holds `target`'s role, connected. */
  async resolveTarget(target: BrokerTarget): Promise<Broker> {
    switch (target.kind) {
      case 'controller':
        await this.refreshMetadataIfNecessary();
        return this.findControllerBroker();
      case 'coordinator':
        return this.findGroupCoordinator({ groupId: target.key, coordinatorType: target.coordinatorType });
      case 'leader': {
        const { topic, partition } = target;
        await this.addTargetTopic(topic);
        await this.refreshMetadataIfNecessary();
        const leader = this.findTopicPartitionMetadata(topic).find(
          ({ partitionId }) => partitionId === partition,
        )?.leader;
        if (leader == null || leader < 0) {
          throw new KafkaBrokerNotFound(`Could not find leader for ${topic} partition ${partition}`, {
            retriable: true,
          });
        }
        return this.findBroker({ nodeId: String(leader) });
      }
      case 'node':
        return this.findBroker({ nodeId: String(target.nodeId) });
      case 'any':
        return this.brokerPool.findConnectedBroker();
    }
  }

  /**
   * Carries out the recovery the error classifier picks for `error`: refreshes stale metadata
   * (also for a moved coordinator, which may be a broker the metadata does not list yet), drops
   * the broker of a closed connection, or reconnects. Does nothing for any other error.
   */
  async recover(error: unknown): Promise<void> {
    switch (recoveryAction(error)) {
      case RECOVERY_ACTIONS.REFRESH_METADATA:
      case RECOVERY_ACTIONS.FIND_COORDINATOR:
        await this.#refreshStaleMetadata();
        return;
      case RECOVERY_ACTIONS.DROP_BROKER:
        this.onConnectionClosed(error);
        return;
      case RECOVERY_ACTIONS.RECONNECT:
        if (!this.isConnected()) await this.connect();
        await this.#refreshStaleMetadata();
        return;
      case RECOVERY_ACTIONS.GIVE_UP:
        return;
    }
  }

  /**
   * Drops the broker whose connection closed, when `error` is a closed connection naming its
   * address, so the next lookup reconnects or rediscovers it. Returns whether it dropped one.
   */
  onConnectionClosed(error: unknown): boolean {
    const address = closedConnectionAddress(error);
    if (!address) return false;
    this.brokerPool.removeBroker(address);
    return true;
  }

  async connect(): Promise<void> {
    await this.#connect();
  }

  async disconnect(): Promise<void> {
    await this.#telemetry?.stop();
    await this.brokerPool.disconnect();
  }

  /** KIP-714 client instance UUID, or `null` until the broker assigns one. */
  clientInstanceId(): Buffer | null {
    return this.#telemetry?.clientInstanceId() ?? null;
  }

  recordProduceMetrics(metrics: ProduceMetrics): void {
    this.#telemetry?.recordProduce(metrics);
  }

  /**
   * KIP-951: patches the cached partition leader from a Produce/Fetch error response instead of
   * a full `refreshMetadata`. Returns whether the cached metadata was actually patched.
   */
  async applyLeaderUpdate(options: ApplyLeaderUpdateOptions): Promise<boolean> {
    return this.brokerPool.applyLeaderUpdate(options);
  }

  async refreshMetadata(): Promise<void> {
    await this.#refreshMetadata();
  }

  /**
   * Refreshes metadata after an error showed it stale: marks it expired, then refreshes through
   * the update queue's staleness re-check, so callers recovering at once share one Metadata request.
   */
  refreshStaleMetadata(): Promise<void> {
    return this.#refreshStaleMetadata();
  }

  /** Called before every produce attempt; returns the shared promise directly to keep that path short. */
  refreshMetadataIfNecessary(): Promise<void> {
    return this.#refreshMetadataIfNecessary();
  }

  async metadata(
    options: { topics?: readonly string[]; forceRefresh?: boolean } = {},
  ): Promise<ClusterMetadata | null> {
    const topics = options.topics ?? [];
    return this.retrier(async (bail) => {
      try {
        if (options.forceRefresh) {
          await this.brokerPool.refreshMetadata([...topics]);
        } else {
          await this.brokerPool.refreshMetadataIfNecessary([...topics]);
        }
        return await this.brokerPool.withBroker(async ({ broker }) =>
          broker.send(Metadata, { topics: [...topics], allowAutoTopicCreation: broker.allowAutoTopicCreation }),
        );
      } catch (e) {
        const error = e as Error;
        if (isStaleMetadata(error)) {
          throw error;
        }
        bail(error);
        return null;
      }
    });
  }

  async addTargetTopic(topic: string): Promise<void> {
    return this.addMultipleTargetTopics([topic]);
  }

  /**
   * Whether `topics` are all target topics and metadata is loaded, so `addMultipleTargetTopics`
   * would return without work. Lets the produce path skip that await.
   */
  hasLoadedTargetTopics(topics: readonly string[]): boolean {
    return this.brokerPool.metadata != null && topics.every((topic) => this.targetTopics.has(topic));
  }

  /** Whether `refreshMetadataIfNecessary` would fetch metadata for the target topics. */
  needsMetadataRefresh(): boolean {
    return this.brokerPool.needsMetadataRefresh(this.targetTopics);
  }

  /** The broker for `nodeId` when it is already connected; `findBroker` connects or rediscovers it otherwise. */
  connectedBroker(nodeId: string): Broker | undefined {
    return this.brokerPool.connectedBroker(nodeId);
  }

  async addMultipleTargetTopics(topics: readonly string[]): Promise<void> {
    if (this.hasLoadedTargetTopics(topics)) {
      return;
    }

    await this.mutatingTargetTopics.acquire();

    try {
      // Recheck under the lock so a concurrent first-seen add cannot drop a topic.
      if (this.hasLoadedTargetTopics(topics)) {
        return;
      }

      const previousSize = this.targetTopics.size;
      const previousTopics = new Set(this.targetTopics);
      for (const topic of topics) {
        this.targetTopics.add(topic);
      }

      const hasChanged = previousSize !== this.targetTopics.size || !this.brokerPool.metadata;

      if (hasChanged) {
        try {
          await this.refreshMetadata();
        } catch (e) {
          const error = e as Error & { topic?: string };
          if (isUnknownTopic(error) || isTopicAuthorizationFailed(error)) {
            if (error.topic && previousTopics.has(error.topic) === false) {
              this.targetTopics.delete(error.topic);
            } else {
              this.targetTopics = previousTopics;
            }
          }

          throw error;
        }
      }
    } finally {
      await this.mutatingTargetTopics.release();
    }
  }

  getNodeIds(): string[] {
    return this.brokerPool.getNodeIds();
  }

  async findBroker({ nodeId }: { nodeId: string }): Promise<Broker> {
    try {
      return await this.brokerPool.findBroker({ nodeId });
    } catch (e) {
      // The client probably has stale metadata.
      const { name } = e as Error;
      if (name === 'KafkaBrokerNotFound' || name === 'KafkaLockTimeout' || isConnectionError(e)) {
        await this.refreshMetadata();
      }

      throw e;
    }
  }

  async findControllerBroker(): Promise<Broker> {
    return this.#findControllerBroker();
  }

  findTopicPartitionMetadata(topic: string): PartitionMetadata[] {
    const { metadata } = this.brokerPool;
    if (!metadata) {
      throw new KafkaTopicMetadataNotLoaded('Topic metadata not loaded', { topic });
    }

    const topicMetadata = metadata.topicMetadata.find((t) => t.topic === topic);
    return topicMetadata ? topicMetadata.partitionMetadata : [];
  }

  /** KIP-516 topic UUID from the last metadata fetch, or `undefined` on older brokers. */
  findTopicId(topic: string): Buffer | undefined {
    const { metadata } = this.brokerPool;
    if (!metadata) {
      throw new KafkaTopicMetadataNotLoaded('Topic metadata not loaded', { topic });
    }

    const topicId = metadata.topicMetadata.find((entry) => entry.topic === topic)?.topicId;
    if (topicId == null || topicId.length !== 16 || topicId.equals(ZERO_TOPIC_ID)) {
      return undefined;
    }
    return topicId;
  }

  findLeaderForPartitions(topic: string, partitions: readonly number[]): Record<number, number[]> {
    const partitionMetadata = this.findTopicPartitionMetadata(topic);

    const partitionsByLeader: Record<number, number[]> = {};
    for (const partitionId of partitions) {
      const metadata = partitionMetadata.find((p) => p.partitionId === partitionId);
      if (!metadata) continue;

      if (metadata.leader == null) {
        throw new KafkaError('Invalid partition metadata', { cause: { topic, partitionId, metadata } });
      }

      const leaderPartitions = partitionsByLeader[metadata.leader];
      if (leaderPartitions) leaderPartitions.push(partitionId);
      else partitionsByLeader[metadata.leader] = [partitionId];
    }
    return partitionsByLeader;
  }

  async findGroupCoordinator({
    groupId,
    coordinatorType = COORDINATOR_TYPES.GROUP,
  }: {
    groupId: string;
    coordinatorType?: CoordinatorType;
  }): Promise<Broker> {
    return this.retrier(async (bail, retryCount, retryTime) => {
      try {
        const { coordinator } = await this.findGroupCoordinatorMetadata({ groupId, coordinatorType });
        return await this.findBroker({ nodeId: String(coordinator.nodeId) });
      } catch (e) {
        const error = e as Error;

        // A new broker can join the cluster before we have the chance to refresh metadata.
        if (error.name === 'KafkaBrokerNotFound' || isStaleCoordinator(error)) {
          this.logger.debug(`${error.message}, refreshing metadata and trying again...`, {
            groupId,
            retryCount,
            retryTime,
          });
          await this.#refreshStaleMetadata();
          throw error;
        }

        // A coordinator that went down during maintenance: findBroker already refreshed metadata.
        if (isConnectionRefused(error)) {
          throw error;
        }

        bail(error);
        throw error;
      }
    });
  }

  async findGroupCoordinatorMetadata({
    groupId,
    coordinatorType,
  }: {
    groupId: string;
    coordinatorType?: CoordinatorType;
  }): Promise<FindCoordinatorResponse> {
    const brokerMetadata = await this.brokerPool.withBroker(async ({ nodeId, broker }) => {
      return this.retrier(async (bail, retryCount, retryTime) => {
        try {
          const brokerMetadata = await broker.send(FindCoordinator, { groupId, coordinatorType });
          this.logger.debug('Found group coordinator', {
            broker: brokerMetadata.coordinator.host,
            nodeId: brokerMetadata.coordinator.nodeId,
          });
          return brokerMetadata;
        } catch (e) {
          const error = e as Error;
          this.logger.debug('Tried to find group coordinator', { nodeId, groupId, error });

          if (isStaleCoordinator(error)) {
            this.logger.debug('Group coordinator not available, retrying...', { nodeId, retryCount, retryTime });
            throw error;
          }

          bail(error);
          throw error;
        }
      });
    });

    if (brokerMetadata) {
      return brokerMetadata;
    }

    throw new KafkaGroupCoordinatorNotFound('Failed to find group coordinator');
  }

  defaultOffset({ fromBeginning }: { fromBeginning?: boolean }): bigint {
    return BigInt(fromBeginning ? EARLIEST_OFFSET : LATEST_OFFSET);
  }

  async fetchTopicsOffset(topics: readonly FetchTopicsOffsetTopic[]): Promise<TopicOffsets[]> {
    const partitionsPerBroker = new Map<string, Record<string, FetchTopicsOffsetPartition[]>>();
    const topicTimestamps = new Map<string, bigint>();

    for (const { topic, partitions, fromBeginning, fromTimestamp } of topics) {
      const partitionsPerLeader = this.findLeaderForPartitions(
        topic,
        partitions.map((p) => p.partition),
      );
      const timestamp = fromTimestamp ?? this.defaultOffset({ fromBeginning });
      topicTimestamps.set(topic, timestamp);

      for (const [nodeId, partitionIds] of Object.entries(partitionsPerLeader)) {
        const perTopic = partitionsPerBroker.get(nodeId) ?? {};
        perTopic[topic] = partitions.filter((p) => partitionIds.includes(p.partition));
        partitionsPerBroker.set(nodeId, perTopic);
      }
    }

    const requests = [...partitionsPerBroker.entries()].map(async ([nodeId, perTopic]) => {
      const broker = await this.findBroker({ nodeId });

      const { responses: topicOffsets } = await broker.send(ListOffsets, {
        isolationLevel: this.isolationLevel,
        topics: Object.entries(perTopic).map(([topic, partitions]) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({ partition, timestamp: topicTimestamps.get(topic) })),
        })),
      });

      return topicOffsets;
    });

    const responses = await Promise.all(requests);
    const partitionsPerTopic = new Map<string, TopicOffsetPartition[]>();

    for (const topicOffset of responses.flat()) {
      const current = partitionsPerTopic.get(topicOffset.topic) ?? [];
      const nextPartitions = topicOffset.partitions.map(({ partition, offset }) => ({ partition, offset }));
      partitionsPerTopic.set(topicOffset.topic, [...current, ...nextPartitions]);
    }

    return [...partitionsPerTopic.entries()].map(([topic, partitions]) => ({ topic, partitions }));
  }

  /**
   * The offset last committed for a partition through this client: by a consumer of `groupId`, or
   * by a transactional producer sending offsets for it. `undefined` when none is known.
   */
  committedOffset({
    groupId,
    topic,
    partition,
  }: {
    groupId: string;
    topic: string;
    partition: number;
  }): bigint | undefined {
    return this.#committedOffsetsByGroup.get(groupId)?.get(topic)?.get(partition);
  }

  markOffsetAsCommitted({
    groupId,
    topic,
    partition,
    offset,
  }: {
    groupId: string;
    topic: string;
    partition: number;
    offset: bigint;
  }): void {
    let committedTopics = this.#committedOffsetsByGroup.get(groupId);
    if (!committedTopics) {
      committedTopics = new Map();
      this.#committedOffsetsByGroup.set(groupId, committedTopics);
    }
    let committedPartitions = committedTopics.get(topic);
    if (!committedPartitions) {
      committedPartitions = new Map();
      committedTopics.set(topic, committedPartitions);
    }
    committedPartitions.set(partition, offset);
  }

  /** Drops the committed offset known for a partition, so the next lookup asks the coordinator again. */
  forgetCommittedOffset({ groupId, topic, partition }: { groupId: string; topic: string; partition: number }): void {
    this.#committedOffsetsByGroup.get(groupId)?.get(topic)?.delete(partition);
  }

  /**
   * Topic names from a Metadata request with an empty topic list (all topics the broker
   * is willing to describe). Connects if needed.
   */
  async listTopics(): Promise<string[]> {
    await this.connect();
    const metadata = await this.metadata({ topics: [] });
    if (!metadata) {
      throw new KafkaMetadataNotLoaded('Topic metadata not loaded');
    }
    return metadata.topicMetadata.flatMap((topic) => (topic.topic == null ? [] : [topic.topic]));
  }

  /**
   * Partition map for `topic` from cached cluster metadata, refreshing if stale.
   * Connects and adds the topic to the refresh set if needed.
   */
  async partitionsFor(topic: string): Promise<TopicPartitionInfo[]> {
    await this.connect();
    await this.addTargetTopic(topic);
    return this.findTopicPartitionMetadata(topic).map((partition) => ({
      topic,
      partitionId: partition.partitionId,
      leader: partition.leader,
      replicas: [...partition.replicas],
      isr: [...partition.isr],
      offlineReplicas: [...(partition.offlineReplicas ?? [])],
    }));
  }
}

/** Public partition description returned by {@link Cluster.partitionsFor}. */
export interface TopicPartitionInfo {
  topic: string;
  partitionId: number;
  leader: number;
  replicas: number[];
  isr: number[];
  offlineReplicas: number[];
}

export type { MetadataBroker, PartitionMetadata };
