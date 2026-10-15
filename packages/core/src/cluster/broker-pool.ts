import { Broker } from '../broker/index';
import { supportsDescribeClusterControllers } from '../broker/capabilities';
import { KafkaBrokerNotFound, KafkaConnectionError, KafkaNonRetriableError, KafkaProtocolError } from '../errors';
import type { Logger } from '../loggers/index';
import { ENDPOINT_TYPES } from '../protocol/enums/endpoint-types';
import { staleMetadata } from '../protocol/error-codes';
import type { BrokerVersions } from '../protocol/requests/index';
import type { ClusterMetadata } from '../protocol/requests/metadata/shared';
import { arrayDiff } from '../utils/array-diff';
import { shuffle } from '../utils/shuffle';
import type { RetryOptions } from '../retry/index';
import { retrier } from '../retry/index';
import type { ConnectionPoolBuilder } from './connection-pool-builder';

function hasBrokerBeenReplaced(
  broker: Broker,
  { host, port, rack }: { host: string; port: number; rack: string | null },
): boolean {
  return (
    broker.connectionPool.host !== host || broker.connectionPool.port !== port || broker.connectionPool.rack !== rack
  );
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
  readonly usingBootstrapControllers: boolean;

  /** Tail of the queue that serializes read-await-write updates of `brokers`. */
  #brokersUpdate: Promise<unknown> = Promise.resolve();
  /**
   * Bumped by `disconnect()`, which is not queued (it would wait out a refresh's retries); an
   * update that sees it change while awaiting discards its result instead of reinstalling brokers.
   */
  #generation = 0;

  readonly #createBroker: (options: {
    connectionPool: Broker['connectionPool'];
    logger: Logger;
    versions?: BrokerVersions | null;
    nodeId?: number | null;
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
      } catch (e) {
        const error = e as Error & { name: string; type?: string; retriable?: boolean };

        if (e instanceof KafkaConnectionError || error.type === 'ILLEGAL_SASL_STATE') {
          // The connection pool builder always rotates the seed broker.
          await this.createSeedBroker();
          this.logger.error(`Failed to connect to seed broker, trying another broker from the list: ${error.message}`, {
            retryCount,
            retryTime,
          });
        } else {
          this.logger.error(error.message, { retryCount, retryTime });
        }

        if (error.retriable) throw error;
        bail(error);
      }
    });
  }

  async disconnect(): Promise<void> {
    this.#generation++;
    if (this.seedBroker) await this.seedBroker.disconnect();
    await Promise.all(Object.values(this.brokers).map((broker) => broker.disconnect()));

    this.brokers = {};
    this.metadata = null;
    this.versions = null;
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
    await this.retrier(async (bail, _retryCount, _retryTime) => {
      try {
        const broker = await this.findConnectedBroker();
        const seedHost = this.seedBroker!.connectionPool.host;
        const seedPort = this.seedBroker!.connectionPool.port;

        const metadata = await this.fetchClusterMetadata(broker, topics);
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
            this.seedBroker!.connectionPool.rack = rack;
            nextBrokers[key] = this.seedBroker!;
            continue;
          }

          nextBrokers[key] = this.#createBroker({
            logger: this.rootLogger,
            versions: this.versions,
            connectionPool: await this.connectionPoolBuilder.build({ host, port, rack }),
            nodeId,
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
          throw new KafkaNonRetriableError('Broker pool disconnected during metadata refresh');
        }

        // removeBroker() may have dropped a broker this refresh kept; don't resurrect it.
        for (const [nodeId, broker] of Object.entries(previousBrokers)) {
          if (nextBrokers[nodeId] === broker && this.brokers[nodeId] !== broker) delete nextBrokers[nodeId];
        }
        this.brokers = nextBrokers;
      } catch (e) {
        const error = e as Error & { type?: string; name?: string };

        if (generation !== this.#generation) {
          bail(error);
          return;
        }

        if (
          this.metadataRecovery === 'rebootstrap' &&
          (error.type === 'REBOOTSTRAP_REQUIRED' || e instanceof KafkaConnectionError)
        ) {
          this.logger.warn('Rediscovering the cluster from the bootstrap broker list', {
            reason: error.type ?? error.name,
          });
          await this.rebootstrap();
          throw new KafkaProtocolError({ message: error.message, retriable: true });
        }

        if (staleMetadata(error)) {
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
    await Promise.all(Object.values(this.brokers).map((broker) => broker.disconnect()));
    this.brokers = {};
    this.metadata = null;
    this.metadataExpireAt = null;

    await this.createSeedBroker();
    await this.connect();
  }

  async refreshMetadataIfNecessary(topics: readonly string[]): Promise<void> {
    const metadata = this.metadata;
    // KIP-919 controller pools have no topic metadata; skip the topic-presence check.
    // Otherwise require a cache hit before reading `topicMetadata` — the previous
    // `this.metadata!` form threw when connect() had not yet loaded Metadata.
    const topicsReady =
      this.usingBootstrapControllers ||
      (metadata != null &&
        topics.every((topic) => metadata.topicMetadata.some((topicMetadata) => topicMetadata.topic === topic)));
    const shouldRefresh =
      metadata == null || this.metadataExpireAt == null || Date.now() > this.metadataExpireAt || !topicsReady;

    if (shouldRefresh) {
      await this.refreshMetadata(topics);
    }
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
        connectionPool: await this.connectionPoolBuilder.build(endpoint),
        nodeId: endpoint.nodeId,
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

  getNodeIds(): string[] {
    return Object.keys(this.brokers);
  }

  /**
   * Metadata against brokers, or DescribeCluster (`endpointType=CONTROLLER`) when this pool
   * was created for Admin `bootstrapControllers` (KIP-919).
   */
  private async fetchClusterMetadata(broker: Broker, topics: readonly string[]): Promise<ClusterMetadata> {
    if (!this.usingBootstrapControllers) {
      return broker.metadata([...topics]);
    }

    const versions = broker.versions ?? this.versions ?? {};
    if (!supportsDescribeClusterControllers(versions)) {
      throw new KafkaNonRetriableError('bootstrapControllers requires DescribeCluster v1 (KIP-919, Kafka 3.7+)');
    }

    const body = await broker.describeCluster({
      includeClusterAuthorizedOperations: false,
      endpointType: ENDPOINT_TYPES.CONTROLLER,
    });
    return clusterMetadataFromDescribeCluster(body);
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
        const error = e as Error & { name: string; type?: string; retriable?: boolean };

        const isConnectionError = e instanceof KafkaConnectionError;
        if (isConnectionError || error.type === 'ILLEGAL_SASL_STATE') {
          await broker.disconnect();
        }

        // To avoid reconnecting to an unavailable host, bail on connection errors and refresh
        // metadata on a higher level before reconnecting.
        if (isConnectionError) {
          bail(error);
          return;
        }

        if (error.type === 'ILLEGAL_SASL_STATE') {
          // Rebuild the connection pool since it can't recover from illegal SASL state.
          broker.connectionPool = await this.connectionPoolBuilder.build({
            host: broker.connectionPool.host,
            port: broker.connectionPool.port,
            rack: broker.connectionPool.rack,
          });

          this.logger.error('Failed to connect to broker, reconnecting', { retryCount, retryTime });
          throw new KafkaProtocolError({ message: error.message, retriable: true });
        }

        if (error.retriable) throw error;
        this.logger.error(error.message, { retryCount, retryTime, stack: error.stack });
        bail(error);
      }
    });
  }
}
