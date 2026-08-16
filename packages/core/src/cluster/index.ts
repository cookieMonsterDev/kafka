import { Broker } from '../broker/index.js';
import { EARLIEST_OFFSET, LATEST_OFFSET } from '../constants.js';
import {
  KafkaJSBrokerNotFound,
  KafkaJSError,
  KafkaJSGroupCoordinatorNotFound,
  KafkaJSMetadataNotLoaded,
  KafkaJSTopicMetadataNotLoaded,
} from '../errors.js';
import type { InstrumentationEventEmitter } from '../instrumentation/emitter.js';
import type { Logger } from '../loggers/index.js';
import type { ConnectionOptions } from '../network/connection.js';
import type { SocketFactory } from '../network/socket-factory.js';
import { COORDINATOR_TYPES } from '../protocol/enums/coordinator-types.js';
import type { CoordinatorType } from '../protocol/enums/coordinator-types.js';
import type { IsolationLevel } from '../protocol/enums/isolation-level.js';
import type { FindCoordinatorResponseV2Body } from '../protocol/requests/find-coordinator/v2/response.js';
import type { MetadataResponseV6Body } from '../protocol/requests/metadata/v6/response.js';
import type { RetryOptions } from '../retry/index.js';
import { retrier } from '../retry/index.js';
import { Lock } from '../utils/lock.js';
import { sharedPromiseTo } from '../utils/shared-promise-to.js';
import { BrokerPool } from './broker-pool.js';
import { connectionPoolBuilder } from './connection-pool-builder.js';

type MetadataBroker = MetadataResponseV6Body['brokers'][number];
type MetadataTopic = MetadataResponseV6Body['topicMetadata'][number];
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

export type CommittedOffsetsByGroup = Map<string, Record<string, Record<string, bigint>>>;

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
  retry?: RetryOptions;
  allowAutoTopicCreation?: boolean;
  maxInFlightRequests?: number | null;
  isolationLevel?: IsolationLevel;
  instrumentationEmitter?: InstrumentationEventEmitter | null;
  offsets?: CommittedOffsetsByGroup;
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
  readonly committedOffsetsByGroup: CommittedOffsetsByGroup;

  targetTopics = new Set<string>();
  readonly mutatingTargetTopics: Lock;

  readonly #connect: () => Promise<void>;
  readonly #refreshMetadata: () => Promise<void>;
  readonly #refreshMetadataIfNecessary: () => Promise<void>;
  readonly #findControllerBroker: () => Promise<Broker>;

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
    retry,
    allowAutoTopicCreation,
    maxInFlightRequests,
    isolationLevel,
    instrumentationEmitter = null,
    offsets = new Map(),
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
    });

    this.targetTopics = new Set();
    this.mutatingTargetTopics = new Lock({ description: 'updating target topics', timeout: requestTimeout });
    this.isolationLevel = isolationLevel;
    this.brokerPool = new BrokerPool({
      connectionPoolBuilder: builder,
      logger: this.rootLogger,
      retry,
      allowAutoTopicCreation,
      authenticationTimeout,
      metadataMaxAge,
    });
    this.committedOffsetsByGroup = offsets;

    this.#connect = sharedPromiseTo(async () => {
      await this.brokerPool.connect();
    });

    this.#refreshMetadata = sharedPromiseTo(async () => {
      await this.brokerPool.refreshMetadata([...this.targetTopics]);
    });

    this.#refreshMetadataIfNecessary = sharedPromiseTo(async () => {
      await this.brokerPool.refreshMetadataIfNecessary([...this.targetTopics]);
    });

    this.#findControllerBroker = sharedPromiseTo(async () => {
      const { metadata } = this.brokerPool;

      if (!metadata || metadata.controllerId == null) {
        throw new KafkaJSMetadataNotLoaded('Topic metadata not loaded');
      }

      const broker = await this.findBroker({ nodeId: String(metadata.controllerId) });

      if (!broker) {
        throw new KafkaJSBrokerNotFound(
          `Controller broker with id ${metadata.controllerId} not found in the cached metadata`,
        );
      }

      return broker;
    });
  }

  isConnected(): boolean {
    return this.brokerPool.hasConnectedBrokers();
  }

  async connect(): Promise<void> {
    await this.#connect();
  }

  async disconnect(): Promise<void> {
    await this.brokerPool.disconnect();
  }

  removeBroker({ host, port }: { host: string; port: number }): void {
    this.brokerPool.removeBroker({ host, port });
  }

  async refreshMetadata(): Promise<void> {
    await this.#refreshMetadata();
  }

  async refreshMetadataIfNecessary(): Promise<void> {
    await this.#refreshMetadataIfNecessary();
  }

  async metadata(options: { topics?: readonly string[] } = {}): Promise<MetadataResponseV6Body | null> {
    const topics = options.topics ?? [];
    return this.retrier(async (bail) => {
      try {
        await this.brokerPool.refreshMetadataIfNecessary([...topics]);
        return await this.brokerPool.withBroker(async ({ broker }) => broker.metadata([...topics]));
      } catch (e) {
        const error = e as Error & { type?: string };
        if (error.type === 'LEADER_NOT_AVAILABLE') {
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

  async addMultipleTargetTopics(topics: readonly string[]): Promise<void> {
    await this.mutatingTargetTopics.acquire();

    try {
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
          const error = e as Error & { type?: string };
          if (
            error.type === 'INVALID_TOPIC_EXCEPTION' ||
            error.type === 'UNKNOWN_TOPIC_OR_PARTITION' ||
            error.type === 'TOPIC_AUTHORIZATION_FAILED'
          ) {
            this.targetTopics = previousTopics;
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
      const error = e as Error & { name: string };
      // The client probably has stale metadata.
      if (
        error.name === 'KafkaJSBrokerNotFound' ||
        error.name === 'KafkaJSLockTimeout' ||
        error.name === 'KafkaJSConnectionError'
      ) {
        await this.refreshMetadata();
      }

      throw error;
    }
  }

  async findControllerBroker(): Promise<Broker> {
    return this.#findControllerBroker();
  }

  findTopicPartitionMetadata(topic: string): PartitionMetadata[] {
    const { metadata } = this.brokerPool;
    if (!metadata) {
      throw new KafkaJSTopicMetadataNotLoaded('Topic metadata not loaded', { topic });
    }

    const topicMetadata = metadata.topicMetadata.find((t) => t.topic === topic);
    return topicMetadata ? topicMetadata.partitionMetadata : [];
  }

  findLeaderForPartitions(topic: string, partitions: readonly number[]): Record<number, number[]> {
    const partitionMetadata = this.findTopicPartitionMetadata(topic);

    return partitions.reduce<Record<number, number[]>>((result, partitionId) => {
      const metadata = partitionMetadata.find((p) => p.partitionId === partitionId);
      if (!metadata) return result;

      if (metadata.leader == null) {
        throw new KafkaJSError('Invalid partition metadata', { cause: { topic, partitionId, metadata } });
      }

      const current = result[metadata.leader] ?? [];
      return { ...result, [metadata.leader]: [...current, partitionId] };
    }, {});
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
        const error = e as Error & { name: string; type?: string; code?: string };

        // A new broker can join the cluster before we have the chance to refresh metadata.
        if (error.name === 'KafkaJSBrokerNotFound' || error.type === 'GROUP_COORDINATOR_NOT_AVAILABLE') {
          this.logger.debug(`${error.message}, refreshing metadata and trying again...`, {
            groupId,
            retryCount,
            retryTime,
          });
          await this.refreshMetadata();
          throw error;
        }

        if (error.code === 'ECONNREFUSED') {
          // During maintenance the current coordinator can go down; findBroker will refresh
          // metadata and re-throw the error. findGroupCoordinator has to re-throw the error to go
          // through the retry cycle.
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
  }): Promise<FindCoordinatorResponseV2Body> {
    const brokerMetadata = await this.brokerPool.withBroker(async ({ nodeId, broker }) => {
      return this.retrier(async (bail, retryCount, retryTime) => {
        try {
          const brokerMetadata = await broker.findGroupCoordinator({ groupId, coordinatorType });
          this.logger.debug('Found group coordinator', {
            broker: brokerMetadata.coordinator.host,
            nodeId: brokerMetadata.coordinator.nodeId,
          });
          return brokerMetadata;
        } catch (e) {
          const error = e as Error & { type?: string };
          this.logger.debug('Tried to find group coordinator', { nodeId, groupId, error });

          if (error.type === 'GROUP_COORDINATOR_NOT_AVAILABLE') {
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

    throw new KafkaJSGroupCoordinatorNotFound('Failed to find group coordinator');
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

      const { responses: topicOffsets } = await broker.listOffsets({
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

  committedOffsets({ groupId }: { groupId: string }): Record<string, Record<string, bigint>> {
    if (!this.committedOffsetsByGroup.has(groupId)) {
      this.committedOffsetsByGroup.set(groupId, {});
    }

    return this.committedOffsetsByGroup.get(groupId)!;
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
    const committedOffsets = this.committedOffsets({ groupId });
    committedOffsets[topic] = committedOffsets[topic] ?? {};
    committedOffsets[topic][partition] = offset;
  }
}

export type { MetadataBroker, PartitionMetadata };
