import type { ConnectionOptions as TlsConnectionOptions } from 'node:tls';
import { mergeConfigLayers } from '@cookiemonsterdev/kafka-config';
import { createAdmin } from './admin/index';
import type { Admin } from './admin/types';
import { Cluster, type CommittedOffsetsByGroup } from './cluster/index';
import {
  buildKafkaConfigSource,
  resolveKafkaConfig,
  resolveKafkaConfigAsync,
  resolveKafkaConfigFrom,
  SHALLOW_MERGE_KEYS,
  type KafkaConfigSource,
  type KafkaFileConfig,
  type ResolveKafkaConfigResult,
} from './config/index';
import { createConsumer, type Consumer } from './consumer/index';
import { InstrumentationEventEmitter } from './instrumentation/emitter';
import { createMetricsRecorder, type MetricsRecorder } from './instrumentation/metrics';
import { consoleLogCreator } from './loggers/console';
import { createLogger, LOG_LEVELS, type Logger } from './loggers/index';
import { createDefaultSocketFactory } from './network/socket-factory';
import { createProducer, type Producer } from './producer/index';
import { createShareConsumer, type ShareConsumer } from './share-consumer/index';
import { ISOLATION_LEVEL, type IsolationLevel } from './protocol/enums/isolation-level';
import type { AdminConfig, ConsumerConfig, KafkaConfig, ProducerConfig, ShareConsumerConfig } from './types/index';
import { once } from './utils/once';

/** Merges a file section (`producer`/`consumer`/`shareConsumer`/`admin`) under an explicit call argument — the explicit argument always wins. */
function mergeFileSection<T>(explicit: T, fileSection: Partial<T> | undefined): T {
  if (fileSection === undefined) return explicit;
  return mergeConfigLayers<T & Record<string, unknown>>(
    explicit as T & Record<string, unknown>,
    fileSection as (T & Record<string, unknown>) | undefined,
    { shallowMergeKeys: SHALLOW_MERGE_KEYS },
  ) as T;
}

const DEFAULT_METADATA_MAX_AGE = 300_000;

interface CreateClusterOptions {
  metadataMaxAge?: number;
  allowAutoTopicCreation?: boolean;
  maxInFlightRequests?: number | null;
  instrumentationEmitter?: InstrumentationEventEmitter | null;
  isolationLevel?: IsolationLevel;
  brokers?: KafkaConfig['brokers'];
  usingBootstrapControllers?: boolean;
}

function normalizeSsl(ssl: KafkaConfig['ssl']): TlsConnectionOptions | null {
  if (ssl === true) return {};
  if (ssl === false || ssl == null) return null;
  return ssl;
}

/**
 * Entry point for the client. One instance holds shared logging and a committed-offset map;
 * each `producer()`, `consumer()`, and `admin()` call gets its own cluster connection pool.
 *
 * @see https://kafka.apache.org/43/getting-started/introduction/
 */
export class Kafka {
  readonly #logger: Logger;
  readonly #clusterRetry: KafkaConfig['retry'];
  readonly #offsets: CommittedOffsetsByGroup = new Map();
  readonly #createCluster: (options: CreateClusterOptions) => Cluster;
  readonly #warnOfDefaultPartitioner: (logger: Logger) => void;
  readonly #metrics: MetricsRecorder | null;
  readonly #fileConfig: KafkaFileConfig | null;
  readonly #configSource: KafkaConfigSource;

  /**
   * @param explicit Options passed directly to this call. Anything left unset is filled from a
   * `kafka.config.*` file, per {@link KafkaConfig.config} — see {@link Kafka.configSource} for
   * what won and from where.
   * @param resolved Internal — a pre-resolved config, used by {@link Kafka.fromConfig} and
   * {@link Kafka.from} to avoid resolving (and re-discovering) what they already resolved. Never
   * pass this yourself; call one of those instead of `new Kafka()` if you already have a loaded
   * config.
   */
  constructor(explicit: KafkaConfig = {}, resolved: ResolveKafkaConfigResult = resolveKafkaConfig(explicit)) {
    const {
      brokers,
      ssl,
      sasl,
      clientId,
      connectionTimeout = 1000,
      authenticationTimeout,
      reauthenticationThreshold,
      requestTimeout,
      enforceRequestTimeout = true,
      metadataRecovery,
      retry,
      socketFactory = createDefaultSocketFactory(),
      logLevel = LOG_LEVELS.INFO,
      logCreator = consoleLogCreator,
      metrics,
      connectionsMaxIdleMs,
      socketConnectionSetupTimeoutMaxMs,
      clientDnsLookup,
      reconnectBackoffMs,
      reconnectBackoffMaxMs,
      enableMetricsPush,
    } = resolved.config;
    this.#fileConfig = resolved.fileConfig;
    this.#configSource = buildKafkaConfigSource(explicit, resolved.fileConfig, resolved.path);
    this.#logger = createLogger({ level: logLevel, logCreator });
    this.#clusterRetry = retry;
    this.#metrics = createMetricsRecorder(metrics);
    this.#warnOfDefaultPartitioner = once((logger: Logger) => {
      if (process.env.KAFKA_NO_PARTITIONER_WARNING == null) {
        logger.warn(
          'The default partitioner changed. To retain the previous routing, create the producer with "createPartitioner: Partitioners.LegacyPartitioner". Silence this warning by setting the environment variable "KAFKA_NO_PARTITIONER_WARNING=1"',
        );
      }
    });

    const resolvedSsl = normalizeSsl(ssl);
    const resolvedClientId = clientId ?? '';

    this.#createCluster = ({
      metadataMaxAge,
      allowAutoTopicCreation = true,
      maxInFlightRequests = null,
      instrumentationEmitter = null,
      isolationLevel,
      brokers: brokersOverride,
      usingBootstrapControllers = false,
    }) =>
      new Cluster({
        logger: this.#logger,
        retry: this.#clusterRetry,
        offsets: this.#offsets,
        socketFactory,
        brokers: brokersOverride ?? brokers,
        ssl: resolvedSsl,
        sasl,
        clientId: resolvedClientId,
        connectionTimeout,
        authenticationTimeout,
        reauthenticationThreshold,
        requestTimeout,
        enforceRequestTimeout,
        metadataMaxAge,
        metadataRecovery,
        instrumentationEmitter,
        allowAutoTopicCreation,
        maxInFlightRequests,
        isolationLevel,
        usingBootstrapControllers,
        connectionsMaxIdleMs,
        clientDnsLookup,
        socketConnectionSetupTimeoutMaxMs,
        reconnectBackoffMs,
        reconnectBackoffMaxMs,
        enableMetricsPush,
      });
  }

  /**
   * Create a producer. Idempotent and transactional producers require matching broker support.
   * @see https://kafka.apache.org/43/configuration/producer-configs/
   */
  producer(explicit: ProducerConfig = {}): Producer {
    const {
      createPartitioner,
      retry,
      metadataMaxAge = DEFAULT_METADATA_MAX_AGE,
      allowAutoTopicCreation,
      idempotent,
      transactionalId,
      transactionTimeout,
      maxInFlightRequests = 5,
      acks,
      compression,
      compressionLevel,
      lingerMs,
      batchSize,
      bufferMemory,
      deliveryTimeoutMs,
      maxRequestSize,
      hooks,
    } = mergeFileSection(explicit, this.#fileConfig?.producer);
    const instrumentationEmitter = new InstrumentationEventEmitter();
    this.#metrics?.bind(instrumentationEmitter, 'producer');
    const cluster = this.#createCluster({
      metadataMaxAge,
      allowAutoTopicCreation,
      maxInFlightRequests,
      instrumentationEmitter,
    });

    if (createPartitioner == null) {
      this.#warnOfDefaultPartitioner(this.#logger);
    }

    return createProducer({
      retry: { ...this.#clusterRetry, ...retry },
      logger: this.#logger,
      cluster,
      createPartitioner,
      idempotent,
      transactionalId,
      transactionTimeout,
      instrumentationEmitter,
      acks,
      compression,
      compressionLevel,
      lingerMs,
      batchSize,
      bufferMemory,
      deliveryTimeoutMs,
      maxRequestSize,
      hooks,
      metrics: this.#metrics,
    });
  }

  /**
   * Create a consumer in the given group.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/
   */
  consumer(explicit: ConsumerConfig): Consumer {
    const {
      groupId,
      partitionAssigners,
      metadataMaxAge = DEFAULT_METADATA_MAX_AGE,
      sessionTimeout,
      rebalanceTimeout,
      heartbeatInterval,
      maxBytesPerPartition,
      minBytes,
      maxBytes,
      maxWaitTimeInMs,
      retry = { retries: 5 },
      allowAutoTopicCreation,
      maxInFlightRequests,
      readUncommitted = false,
      rackId = '',
      groupInstanceId,
      autoOffsetReset,
      groupProtocol,
      groupRemoteAssignor,
      hooks,
      checkCrcs,
    } = mergeFileSection(explicit, this.#fileConfig?.consumer);
    const isolationLevel = readUncommitted ? ISOLATION_LEVEL.READ_UNCOMMITTED : ISOLATION_LEVEL.READ_COMMITTED;
    const instrumentationEmitter = new InstrumentationEventEmitter();
    this.#metrics?.bind(instrumentationEmitter, 'consumer');
    const cluster = this.#createCluster({
      metadataMaxAge,
      allowAutoTopicCreation,
      maxInFlightRequests,
      isolationLevel,
      instrumentationEmitter,
    });

    return createConsumer({
      retry: { ...this.#clusterRetry, ...retry },
      logger: this.#logger,
      cluster,
      groupId,
      partitionAssigners,
      sessionTimeout,
      rebalanceTimeout,
      heartbeatInterval,
      maxBytesPerPartition,
      minBytes,
      maxBytes,
      maxWaitTimeInMs,
      isolationLevel,
      instrumentationEmitter,
      rackId,
      metadataMaxAge,
      groupInstanceId,
      autoOffsetReset,
      groupProtocol,
      groupRemoteAssignor,
      hooks,
      checkCrcs,
    });
  }

  /**
   * Create a share-group consumer (KIP-932, Kafka 4.0+).
   * @see https://kafka.apache.org/43/configuration/consumer-configs/
   */
  shareConsumer(explicit: ShareConsumerConfig): ShareConsumer {
    const {
      groupId,
      heartbeatInterval,
      maxWaitTimeInMs,
      minBytes,
      maxBytes,
      maxRecords,
      batchSize,
      shareAcquireMode,
      rackId,
      retry,
      metadataMaxAge = DEFAULT_METADATA_MAX_AGE,
      allowAutoTopicCreation,
      maxInFlightRequests,
    } = mergeFileSection(explicit, this.#fileConfig?.shareConsumer);
    const instrumentationEmitter = new InstrumentationEventEmitter();
    this.#metrics?.bind(instrumentationEmitter, 'share_consumer');
    const cluster = this.#createCluster({
      metadataMaxAge,
      allowAutoTopicCreation,
      maxInFlightRequests,
      instrumentationEmitter,
    });

    return createShareConsumer({
      retry: { ...this.#clusterRetry, ...retry },
      logger: this.#logger,
      cluster,
      groupId,
      heartbeatInterval,
      maxWaitTimeInMs,
      minBytes,
      maxBytes,
      maxRecords,
      batchSize,
      shareAcquireMode,
      rackId,
      instrumentationEmitter,
    });
  }

  /**
   * Create an admin client for topics, groups, ACLs, configs, and reassignments.
   * @see https://kafka.apache.org/43/configuration/admin-configs/
   * @see https://kafka.apache.org/43/operations/basic-kafka-operations/
   */
  admin(explicit: AdminConfig = {}): Admin {
    const { retry, bootstrapControllers } = mergeFileSection(explicit, this.#fileConfig?.admin);
    const instrumentationEmitter = new InstrumentationEventEmitter();
    this.#metrics?.bind(instrumentationEmitter, 'admin');
    const cluster = this.#createCluster({
      allowAutoTopicCreation: false,
      instrumentationEmitter,
      ...(bootstrapControllers != null ? { brokers: bootstrapControllers, usingBootstrapControllers: true } : {}),
    });

    return createAdmin({
      retry: { ...this.#clusterRetry, ...retry },
      logger: this.#logger,
      instrumentationEmitter,
      cluster,
    });
  }

  logger(): Logger {
    return this.#logger;
  }

  /**
   * Reports where each `KafkaConfig` key's value came from: the call to `new Kafka()` /
   * `Kafka.fromConfig()` / `Kafka.from()`, a `kafka.config.*` file, or neither (the constructor's
   * own default). Carries no values, so there is nothing here that needs redacting.
   */
  configSource(): KafkaConfigSource {
    return this.#configSource;
  }

  /**
   * Async sibling of `new Kafka()`, for the cases the synchronous constructor structurally
   * cannot handle: a config file that uses top-level `await`, or exports an async factory. Shares
   * discovery and the merge function with the constructor, so the two paths cannot drift.
   */
  static async fromConfig(overrides: KafkaConfig = {}, options: { cwd?: string } = {}): Promise<Kafka> {
    const resolved = await resolveKafkaConfigAsync(overrides, { cwd: options.cwd });
    return new Kafka(overrides, resolved);
  }

  /**
   * Synchronous sibling of {@link Kafka.fromConfig}, for a caller that already loaded a
   * `kafka.config.*` file itself (via `loadKafkaConfig`/`loadKafkaConfigAsync`) and wants to
   * construct clients from it without discovering or reading the file again.
   */
  static from(fileConfig: KafkaFileConfig, overrides: KafkaConfig = {}): Kafka {
    const config = resolveKafkaConfigFrom(fileConfig, overrides);
    return new Kafka(overrides, { config, path: null, fileConfig });
  }
}
