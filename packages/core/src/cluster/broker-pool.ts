import { Broker } from '../broker/index';
import { KafkaBrokerNotFound, KafkaProtocolError } from '../errors';
import type { Logger } from '../loggers/index';
import { staleMetadata } from '../protocol/error-codes';
import type { BrokerVersions } from '../protocol/requests/index';
import type { MetadataResponseV9Body } from '../protocol/requests/metadata/v9/response';
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

export interface BrokerPoolOptions {
  connectionPoolBuilder: ConnectionPoolBuilder;
  logger: Logger;
  retry?: RetryOptions;
  allowAutoTopicCreation?: boolean;
  authenticationTimeout?: number;
  metadataMaxAge?: number;
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
  readonly retrier: ReturnType<typeof retrier>;

  brokers: Record<string, Broker> = {};
  seedBroker: Broker | undefined;
  metadata: MetadataResponseV9Body | null = null;
  metadataExpireAt: number | null = null;
  versions: BrokerVersions | null = null;

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
  }: BrokerPoolOptions) {
    this.rootLogger = logger;
    this.connectionPoolBuilder = connectionPoolBuilder;
    this.metadataMaxAge = metadataMaxAge ?? 0;
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

        if (error.name === 'KafkaConnectionError' || error.type === 'ILLEGAL_SASL_STATE') {
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

  async refreshMetadata(topics: readonly string[]): Promise<void> {
    const broker = await this.findConnectedBroker();
    const seedHost = this.seedBroker!.connectionPool.host;
    const seedPort = this.seedBroker!.connectionPool.port;

    await this.retrier(async (bail, _retryCount, _retryTime) => {
      try {
        this.metadata = await broker.metadata([...topics]);
        this.metadataExpireAt = Date.now() + this.metadataMaxAge;

        const replacedBrokers: Broker[] = [];
        const nextBrokers: Record<string, Broker> = { ...this.brokers };

        for (const { nodeId, host, port, rack } of this.metadata.brokers) {
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

        const freshBrokerIds = this.metadata.brokers.map(({ nodeId }) => String(nodeId)).sort();
        const currentBrokerIds = Object.keys(nextBrokers).sort();
        const unusedBrokerIds = arrayDiff(currentBrokerIds, freshBrokerIds);

        const brokerDisconnects = unusedBrokerIds.map((nodeId) =>
          nextBrokers[nodeId]!.disconnect().then(() => {
            delete nextBrokers[nodeId];
          }),
        );

        const replacedBrokersDisconnects = replacedBrokers.map((replaced) => replaced.disconnect());
        await Promise.all([...brokerDisconnects, ...replacedBrokersDisconnects]);

        this.brokers = nextBrokers;
      } catch (e) {
        const error = e as Error & { type?: string };
        if (staleMetadata(error)) {
          throw error;
        }
        bail(error);
      }
    });
  }

  async refreshMetadataIfNecessary(topics: readonly string[]): Promise<void> {
    const shouldRefresh =
      this.metadata == null ||
      this.metadataExpireAt == null ||
      Date.now() > this.metadataExpireAt ||
      !topics.every((topic) => this.metadata!.topicMetadata.some((topicMetadata) => topicMetadata.topic === topic));

    if (shouldRefresh) {
      await this.refreshMetadata(topics);
    }
  }

  getNodeIds(): string[] {
    return Object.keys(this.brokers);
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
      const broker = await this.findBroker({ nodeId });
      try {
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

        if (error.name === 'KafkaConnectionError' || error.type === 'ILLEGAL_SASL_STATE') {
          await broker.disconnect();
        }

        // To avoid reconnecting to an unavailable host, bail on connection errors and refresh
        // metadata on a higher level before reconnecting.
        if (error.name === 'KafkaConnectionError') {
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
