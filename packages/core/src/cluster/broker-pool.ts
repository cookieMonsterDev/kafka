import { Broker } from '../broker/index';
import { supportsDescribeClusterControllers } from '../broker/capabilities';
import { KafkaBrokerNotFound, KafkaConnectionError, KafkaNonRetriableError, KafkaProtocolError } from '../errors';
import type { Logger } from '../loggers/index';
import { ENDPOINT_TYPES } from '../protocol/enums/endpoint-types';
import type { BrokerVersions } from '../protocol/requests/index';
import type { FinalizedFeatureLevels } from '../protocol/requests/api-versions/feature-fields';
import type { ClusterMetadata } from '../protocol/requests/metadata/shared';
import { arrayDiff } from '../utils/array-diff';
import { shuffle } from '../utils/shuffle';
import type { RetryOptions } from '../retry/index';
import { retrier } from '../retry/index';
import { isIllegalSaslState, isRebootstrapRequired, isStaleMetadata } from './error-classifier';
import type { ConnectionPoolBuilder } from './connection-pool-builder';
import { DescribeCluster } from '../protocol/requests/lazy-families';
import { Metadata } from '../protocol/requests/metadata/index';

function hasAddressChanged(broker: Broker, { host, port }: { host: string; port: number }): boolean {
  return broker.connectionPool.host !== host || broker.connectionPool.port !== port;
}

function hasBrokerBeenReplaced(
  broker: Broker,
  { host, port, rack }: { host: string; port: number; rack: string | null },
): boolean {
  return hasAddressChanged(broker, { host, port }) || broker.connectionPool.rack !== rack;
}

/** KIP-951: the partition's current leader, reported on a stale-leader error response. */
export interface CurrentLeader {
  leaderId: number;
  leaderEpoch: number;
}

/** KIP-951: a broker address the client may not already have cached metadata for. */
export interface NodeEndpointUpdate {
  nodeId: number;
  host: string;
  port: number;
  rack: string | null;
}

export interface ApplyLeaderUpdateOptions {
  topic: string;
  partition: number;
  currentLeader: CurrentLeader;
  nodeEndpoints?: readonly NodeEndpointUpdate[];
}

export type MetadataRecovery = 'rebootstrap' | 'none';

export interface BrokerPoolOptions {
  connectionPoolBuilder: ConnectionPoolBuilder;
  logger: Logger;
  retry?: RetryOptions;
  allowAutoTopicCreation?: boolean;
  authenticationTimeout?: number;
  metadataMaxAge?: number;
  metadataRecovery?: MetadataRecovery;
  /**
   * KIP-919: discover the controller quorum via DescribeCluster (`endpointType=CONTROLLER`)
   * instead of Metadata. Used when Admin is constructed with `bootstrapControllers`.
   */
  usingBootstrapControllers?: boolean;
}

/** Map a DescribeCluster (controller) response onto the Metadata shape `BrokerPool` already stores. */
export function clusterMetadataFromDescribeCluster(body: {
  throttleTime: number;
  clientSideThrottleTime: number;
  brokers: readonly { nodeId: number; host: string; port: number; rack: string | null }[];
  clusterId: string;
  controllerId: number;
  clusterAuthorizedOperations: number;
}): ClusterMetadata {
  return {
    throttleTime: body.throttleTime,
    clientSideThrottleTime: body.clientSideThrottleTime,
    brokers: body.brokers.map(({ nodeId, host, port, rack }) => ({ nodeId, host, port, rack })),
    clusterId: body.clusterId,
    controllerId: body.controllerId,
    topicMetadata: [],
    clusterAuthorizedOperations: body.clusterAuthorizedOperations,
  };
}

/**
 * Owns every `Broker` the client has discovered (one per cluster node), plus the seed broker used
 * to bootstrap before real metadata is available. `refreshMetadata` reconciles this set against a
 * fresh `Metadata` response: unchanged brokers are kept as-is, brokers whose address changed are
 * replaced, and brokers no longer in the cluster are disconnected and dropped.
 */
export class BrokerPool {
  readonly rootLogger: Logger;
  readonly logger: Logger;
  readonly connectionPoolBuilder: ConnectionPoolBuilder;
  readonly metadataMaxAge: number;
  readonly metadataRecovery: MetadataRecovery;
  readonly retrier: ReturnType<typeof retrier>;

  brokers: Record<string, Broker> = {};
  seedBroker: Broker | undefined;
  metadata: ClusterMetadata | null = null;
  metadataExpireAt: number | null = null;
  versions: BrokerVersions | null = null;
  /** The seed broker's finalized feature levels, read when the pool connects. */
  finalizedFeatures: FinalizedFeatureLevels = {};
  readonly usingBootstrapControllers: boolean;

  /** Tail of the queue that serializes read-await-write updates of `brokers`. */
  #brokersUpdate: Promise<unknown> = Promise.resolve();
  /**
   * Bumped by `disconnect()`, which is not queued (it would wait out a refresh's retries); an
   * update that sees it change while awaiting discards its result instead of reinstalling brokers.
   */
  #generation = 0;
  /**
   * KIP-1242: the `REBOOTSTRAP_REQUIRED` a known broker answered on connect, because its address
   * now leads to another broker or cluster. The next metadata refresh rebootstraps on it.
   */
  #misroutedConnectionError: Error | null = null;

  readonly #createBroker: (options: {
    connectionPool: Broker['connectionPool'];
    logger: Logger;
    versions?: BrokerVersions | null;
    finalizedFeatures?: FinalizedFeatureLevels;
    nodeId?: number | null;
    clusterId?: string | null;
  }) => Broker;

  constructor({
    connectionPoolBuilder,
    logger,
    retry,
    allowAutoTopicCreation,
    authenticationTimeout,
    metadataMaxAge,
    metadataRecovery,
    usingBootstrapControllers = false,
  }: BrokerPoolOptions) {
    this.rootLogger = logger;
    this.connectionPoolBuilder = connectionPoolBuilder;
    this.metadataMaxAge = metadataMaxAge ?? 0;
    this.metadataRecovery = metadataRecovery ?? 'rebootstrap';
    this.usingBootstrapControllers = usingBootstrapControllers;
    this.logger = logger.namespace('BrokerPool');
    this.retrier = retrier(retry);

    this.#createBroker = (options) => new Broker({ allowAutoTopicCreation, authenticationTimeout, ...options });
  }

  hasConnectedBrokers(): boolean {
    return (
      Object.values(this.brokers).some((broker) => broker.isConnected()) || (this.seedBroker?.isConnected() ?? false)
    );
  }

  async createSeedBroker(): Promise<void> {
    if (this.seedBroker) {
      await this.seedBroker.disconnect();
    }

    const connectionPool = await this.connectionPoolBuilder.build();
    this.seedBroker = this.#createBroker({ connectionPool, logger: this.rootLogger });
  }

  async connect(): Promise<void> {
    if (this.hasConnectedBrokers()) return;

    if (!this.seedBroker) {
      await this.createSeedBroker();
    }

    await this.retrier(async (bail, retryCount, retryTime) => {
      try {
        await this.seedBroker!.connect();
        this.versions = this.seedBroker!.versions;
        this.finalizedFeatures = this.seedBroker!.finalizedFeatures;
      } catch (e) {
        const error = e as Error & { retriable?: boolean };
        const isMisrouted = isRebootstrapRequired(error);
        if (isMisrouted) this.#recordMisroutedConnection(this.seedBroker!, error);

        if (e instanceof KafkaConnectionError || isIllegalSaslState(error) || isMisrouted) {
          // The connection pool builder always rotates the seed broker. A new one has no node id, so
          // it connects to whichever broker its address leads to.
          await this.createSeedBroker();
          this.logger.error(`Failed to connect to seed broker, trying another broker from the list: ${error.message}`, {
            retryCount,
            retryTime,
          });
        } else {
          this.logger.error(error.message, { retryCount, retryTime });
        }

        if (isMisrouted) throw new KafkaProtocolError({ message: error.message, retriable: true });
        if (error.retriable) throw error;
        bail(error);
      }
    });
  }

  async disconnect(): Promise<void> {
    this.#generation++;
    this.#misroutedConnectionError = null;
    if (this.seedBroker) await this.seedBroker.disconnect();
    await Promise.all(Object.values(this.brokers).map((broker) => broker.disconnect()));

    this.brokers = {};
    this.metadata = null;
    this.versions = null;
    this.finalizedFeatures = {};
  }

  removeBroker({ host, port }: { host: string; port: number }): void {
    const removedBroker = Object.values(this.brokers).find(
      (broker) => broker.connectionPool.host === host && broker.connectionPool.port === port,
    );

    if (removedBroker) {
      delete this.brokers[String(removedBroker.nodeId)];
      this.metadataExpireAt = null;

      if (this.seedBroker?.nodeId === removedBroker.nodeId) {
        this.seedBroker = shuffle(Object.values(this.brokers))[0];
      }
    }
  }

  /**
   * Runs `update` once every earlier one has settled. Refreshes and leader updates snapshot
   * `brokers`, await, then write it back; run concurrently, the last writer would drop (and leak)
   * brokers the others created.
   */
  #serializeBrokersUpdate<T>(update: () => Promise<T>): Promise<T> {
    const run = this.#brokersUpdate.then(update);
    this.#brokersUpdate = run.catch(() => undefined);
    return run;
  }

  refreshMetadata(topics: readonly string[]): Promise<void> {
    return this.#serializeBrokersUpdate(() => this.#refreshMetadata(topics));
  }

  async #refreshMetadata(topics: readonly string[]): Promise<void> {
    const generation = this.#generation;
    const disconnectedError = () => new KafkaNonRetriableError('Broker pool disconnected during metadata refresh');
    await this.retrier(async (bail, _retryCount, _retryTime) => {
      // disconnect() may have landed during a retry backoff; don't reconnect the seed for it.
      if (generation !== this.#generation) {
        bail(disconnectedError());
        return;
      }
      try {
        const broker = await this.findConnectedBroker();
        // A known broker's address led elsewhere, since the last refresh or just now while
        // findConnectedBroker() moved past it; the rest of the metadata may be stale too.
        if (this.#misroutedConnectionError) throw this.#misroutedConnectionError;
        const seedHost = this.seedBroker!.connectionPool.host;
        const seedPort = this.seedBroker!.connectionPool.port;

        const metadata = await this.fetchClusterMetadata(broker, topics);
        const clusterId = this.#expectedClusterId(metadata);
        this.metadata = metadata;
        this.metadataExpireAt = Date.now() + this.metadataMaxAge;

        const replacedBrokers: Broker[] = [];
        const previousBrokers = { ...this.brokers };
        const nextBrokers: Record<string, Broker> = { ...previousBrokers };

        for (const { nodeId, host, port, rack } of metadata.brokers) {
          const key = String(nodeId);
          const existing = nextBrokers[key];

          if (existing) {
            if (!hasBrokerBeenReplaced(existing, { host, port, rack })) {
              continue;
            }
            replacedBrokers.push(existing);
          }

          if (host === seedHost && port === seedPort) {
            this.seedBroker!.nodeId = nodeId;
            this.seedBroker!.clusterId = clusterId;
            this.seedBroker!.connectionPool.rack = rack;
            nextBrokers[key] = this.seedBroker!;
            continue;
          }

          nextBrokers[key] = this.#createBroker({
            logger: this.rootLogger,
            versions: this.versions,
            finalizedFeatures: this.finalizedFeatures,
            connectionPool: await this.connectionPoolBuilder.build({ host, port, rack }),
            nodeId,
            clusterId,
          });
        }

        const freshBrokerIds = metadata.brokers.map(({ nodeId }) => String(nodeId)).sort();
        const currentBrokerIds = Object.keys(nextBrokers).sort();
        const unusedBrokerIds = arrayDiff(currentBrokerIds, freshBrokerIds);

        const brokerDisconnects = unusedBrokerIds.map((nodeId) =>
          nextBrokers[nodeId]!.disconnect().then(() => {
            delete nextBrokers[nodeId];
          }),
        );

        const replacedBrokersDisconnects = replacedBrokers.map((replaced) => replaced.disconnect());
        await Promise.all([...brokerDisconnects, ...replacedBrokersDisconnects]);

        if (generation !== this.#generation) {
          const created = Object.entries(nextBrokers).filter(
            ([nodeId, broker]) => broker !== previousBrokers[nodeId] && broker !== this.seedBroker,
          );
          await Promise.all(created.map(([, broker]) => broker.disconnect()));
          this.metadata = null;
          throw disconnectedError();
        }

        // removeBroker() may have dropped a broker this refresh kept; don't resurrect it.
        for (const [nodeId, broker] of Object.entries(previousBrokers)) {
          if (nextBrokers[nodeId] === broker && this.brokers[nodeId] !== broker) delete nextBrokers[nodeId];
        }
        this.brokers = nextBrokers;
      } catch (e) {
        const error = e as Error & { type?: string };

        if (generation !== this.#generation) {
          // findConnectedBroker() may have reconnected the seed during this attempt.
          await this.seedBroker?.disconnect();
          bail(error);
          return;
        }

        if (
          this.metadataRecovery === 'rebootstrap' &&
          (isRebootstrapRequired(error) || e instanceof KafkaConnectionError)
        ) {
          this.logger.warn('Rediscovering the cluster from the bootstrap broker list', {
            reason: error.type ?? error.name,
          });
          await this.rebootstrap();
          throw new KafkaProtocolError({ message: error.message, retriable: true });
        }

        if (isStaleMetadata(error)) {
          throw error;
        }
        bail(error);
      }
    });
  }

  /**
   * KIP-1102: drops every discovered broker and cached metadata, then rebuilds a seed broker
   * from the original bootstrap list and reconnects. `connectionPoolBuilder.build()` without a
   * destination always draws from that original list rather than brokers this pool has since
   * discovered, so this is a genuine return to the seeds - not a retry against brokers already
   * known to be stale or unreachable.
   */
  async rebootstrap(): Promise<void> {
    this.#misroutedConnectionError = null;
    await Promise.all(Object.values(this.brokers).map((broker) => broker.disconnect()));
    this.brokers = {};
    this.metadata = null;
    this.metadataExpireAt = null;

    await this.createSeedBroker();
    await this.connect();
  }

  /** Marks the cached metadata stale, so the next `refreshMetadataIfNecessary` fetches it. */
  expireMetadata(): void {
    this.metadataExpireAt = null;
  }

  /** Re-checked inside the update queue, so concurrent stale callers share one Metadata request. */
  async refreshMetadataIfNecessary(topics: readonly string[]): Promise<void> {
    if (!this.needsMetadataRefresh(topics)) return;
    await this.#serializeBrokersUpdate(async () => {
      if (this.needsMetadataRefresh(topics)) await this.#refreshMetadata(topics);
    });
  }

  /** Whether the cached metadata is missing, expired, or lacks one of `topics`. */
  needsMetadataRefresh(topics: Iterable<string>): boolean {
    const metadata = this.metadata;
    if (metadata == null || this.metadataExpireAt == null || Date.now() > this.metadataExpireAt) return true;
    // KIP-919 controller pools have no topic metadata; skip the topic-presence check.
    if (this.usingBootstrapControllers) return false;
    for (const topic of topics) {
      if (!metadata.topicMetadata.some((topicMetadata) => topicMetadata.topic === topic)) return true;
    }
    return false;
  }

  /**
   * KIP-951: patches the cached partition leader (and any accompanying broker addresses) from a
   * Produce/Fetch error response's `CurrentLeader` / `NodeEndpoints` tagged fields, without a
   * Metadata RPC. Returns whether the cached partition metadata was actually found and patched -
   * callers should fall back to `refreshMetadata` when it returns `false`.
   */
  applyLeaderUpdate(options: ApplyLeaderUpdateOptions): Promise<boolean> {
    return this.#serializeBrokersUpdate(() => this.#applyLeaderUpdate(options));
  }

  async #applyLeaderUpdate({
    topic,
    partition,
    currentLeader,
    nodeEndpoints = [],
  }: ApplyLeaderUpdateOptions): Promise<boolean> {
    const { metadata } = this;
    if (currentLeader.leaderId < 0 || !metadata) return false;
    const generation = this.#generation;

    for (const endpoint of nodeEndpoints) {
      const key = String(endpoint.nodeId);
      const existing = this.brokers[key];
      if (existing && !hasBrokerBeenReplaced(existing, endpoint)) continue;

      const broker = this.#createBroker({
        logger: this.rootLogger,
        versions: this.versions,
        finalizedFeatures: this.finalizedFeatures,
        connectionPool: await this.connectionPoolBuilder.build(endpoint),
        nodeId: endpoint.nodeId,
        clusterId: this.#expectedClusterId(metadata),
      });
      if (generation !== this.#generation) {
        await broker.disconnect();
        return false;
      }
      const replaced = this.brokers[key];
      this.brokers[key] = broker;
      if (replaced) await replaced.disconnect();

      const brokerIndex = metadata.brokers.findIndex((entry) => entry.nodeId === endpoint.nodeId);
      const brokerEntry = { nodeId: endpoint.nodeId, host: endpoint.host, port: endpoint.port, rack: endpoint.rack };
      if (brokerIndex === -1) {
        metadata.brokers.push(brokerEntry);
      } else {
        metadata.brokers[brokerIndex] = brokerEntry;
      }
    }

    const topicMetadata = metadata.topicMetadata.find((entry) => entry.topic === topic);
    const partitionMetadata = topicMetadata?.partitionMetadata.find((entry) => entry.partitionId === partition);
    if (!partitionMetadata) return false;

    partitionMetadata.leader = currentLeader.leaderId;
    if (currentLeader.leaderEpoch >= 0) partitionMetadata.leaderEpoch = currentLeader.leaderEpoch;
    return true;
  }

  /**
   * Records that `broker` answered `REBOOTSTRAP_REQUIRED` on connect because its address now leads
   * to another broker or cluster (KIP-1242), so the next metadata refresh rebootstraps.
   */
  #recordMisroutedConnection(broker: Broker, error: Error): void {
    this.logger.warn('Broker address leads to another broker or cluster; rediscovering the cluster', {
      broker: broker.brokerAddress,
      nodeId: broker.nodeId,
    });
    this.#misroutedConnectionError = error;
  }

  /**
   * The cluster id brokers are asked to confirm on connect (KIP-1242). Only sent when the pool
   * rebootstraps, the one recovery from a broker that does not confirm it; so `REBOOTSTRAP_REQUIRED`
   * on connect never reaches a pool with `metadataRecovery: 'none'`.
   */
  #expectedClusterId(metadata: ClusterMetadata): string | null {
    return this.metadataRecovery === 'rebootstrap' ? metadata.clusterId : null;
  }

  getNodeIds(): string[] {
    return Object.keys(this.brokers);
  }

  /**
   * Metadata against brokers, or DescribeCluster (`endpointType=CONTROLLER`) when this pool
   * was created for Admin `bootstrapControllers` (KIP-919).
   */
  private async fetchClusterMetadata(broker: Broker, topics: readonly string[]): Promise<ClusterMetadata> {
    if (!this.usingBootstrapControllers) {
      return broker.send(Metadata, { topics: [...topics], allowAutoTopicCreation: broker.allowAutoTopicCreation });
    }

    const versions = broker.versions ?? this.versions ?? {};
    if (!supportsDescribeClusterControllers(versions)) {
      throw new KafkaNonRetriableError('bootstrapControllers requires DescribeCluster v1 (KIP-919, Kafka 3.7+)');
    }

    const body = await broker.send(DescribeCluster, {
      includeClusterAuthorizedOperations: false,
      endpointType: ENDPOINT_TYPES.CONTROLLER,
    });
    return clusterMetadataFromDescribeCluster(body);
  }

  /** Whether the pool holds a broker for `nodeId` at an address other than `host` and `port`. */
  hasBrokerMoved({ nodeId, host, port }: { nodeId: string; host: string; port: number }): boolean {
    const broker = this.brokers[nodeId];
    return broker != null && hasAddressChanged(broker, { host, port });
  }

  /** The broker for `nodeId` when it is already connected; `findBroker` connects it otherwise. */
  connectedBroker(nodeId: string): Broker | undefined {
    const broker = this.brokers[nodeId];
    return broker?.isConnected() ? broker : undefined;
  }

  async findBroker({ nodeId }: { nodeId: string }): Promise<Broker> {
    const broker = this.brokers[nodeId];

    if (!broker) {
      throw new KafkaBrokerNotFound(`Broker ${nodeId} not found in the cached metadata`);
    }

    await this.connectBroker(broker);
    return broker;
  }

  async withBroker<T>(callback: (params: { nodeId: string; broker: Broker }) => Promise<T>): Promise<T | null> {
    const nodeIds = shuffle(Object.keys(this.brokers));
    if (nodeIds.length === 0) {
      throw new KafkaBrokerNotFound('No brokers in the broker pool');
    }

    for (const nodeId of nodeIds) {
      try {
        const broker = await this.findBroker({ nodeId });
        return await callback({ nodeId, broker });
      } catch {
        // try the next broker
      }
    }

    return null;
  }

  async findConnectedBroker(): Promise<Broker> {
    const nodeIds = shuffle(Object.keys(this.brokers));
    const connectedBrokerId = nodeIds.find((nodeId) => this.brokers[nodeId]!.isConnected());

    if (connectedBrokerId) {
      return this.findBroker({ nodeId: connectedBrokerId });
    }

    // Cycle through the nodes until one connects.
    for (const nodeId of nodeIds) {
      try {
        return await this.findBroker({ nodeId });
      } catch {
        // try the next broker
      }
    }

    // Failed to connect to all known brokers, metadata might be old.
    await this.connect();
    return this.seedBroker!;
  }

  private async connectBroker(broker: Broker): Promise<void> {
    if (broker.isConnected()) return;

    await this.retrier(async (bail, retryCount, retryTime) => {
      try {
        await broker.connect();
      } catch (e) {
        const error = e as Error & { retriable?: boolean };

        const isConnectionError = e instanceof KafkaConnectionError;
        const isSaslStateLost = isIllegalSaslState(error);
        if (isConnectionError || isSaslStateLost) {
          await broker.disconnect();
        }

        // To avoid reconnecting to an unavailable host, bail on connection errors and refresh
        // metadata on a higher level before reconnecting.
        if (isConnectionError) {
          bail(error);
          return;
        }

        if (isSaslStateLost) {
          // Rebuild the connection pool since it can't recover from illegal SASL state.
          broker.connectionPool = await this.connectionPoolBuilder.build({
            host: broker.connectionPool.host,
            port: broker.connectionPool.port,
            rack: broker.connectionPool.rack,
          });

          this.logger.error('Failed to connect to broker, reconnecting', { retryCount, retryTime });
          throw new KafkaProtocolError({ message: error.message, retriable: true });
        }

        if (isRebootstrapRequired(error)) {
          // Bail instead of retrying the same address; the next metadata refresh rebootstraps.
          this.#recordMisroutedConnection(broker, error);
          bail(error);
          return;
        }

        if (error.retriable) throw error;
        this.logger.error(error.message, { retryCount, retryTime, stack: error.stack });
        bail(error);
      }
    });
  }
}
