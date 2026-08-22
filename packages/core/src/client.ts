import type { ConnectionOptions as TlsConnectionOptions } from 'node:tls';
import { createAdmin } from './admin/index';
import type { Admin } from './admin/types';
import { Cluster, type CommittedOffsetsByGroup } from './cluster/index';
import { createConsumer, type Consumer } from './consumer/index';
import { InstrumentationEventEmitter } from './instrumentation/emitter';
import { consoleLogCreator } from './loggers/console';
import { createLogger, LOG_LEVELS, type Logger } from './loggers/index';
import { createDefaultSocketFactory } from './network/socket-factory';
import { createProducer, type Producer } from './producer/index';
import { createShareConsumer, type ShareConsumer } from './share-consumer/index';
import { ISOLATION_LEVEL, type IsolationLevel } from './protocol/enums/isolation-level';
import type { AdminConfig, ConsumerConfig, KafkaConfig, ProducerConfig, ShareConsumerConfig } from './types/index';
import { once } from './utils/once';

const DEFAULT_METADATA_MAX_AGE = 300_000;

interface CreateClusterOptions {
  metadataMaxAge?: number;
  allowAutoTopicCreation?: boolean;
  maxInFlightRequests?: number | null;
  instrumentationEmitter?: InstrumentationEventEmitter | null;
  isolationLevel?: IsolationLevel;
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

  constructor({
    brokers,
    ssl,
    sasl,
    clientId,
    connectionTimeout = 1000,
    authenticationTimeout,
    reauthenticationThreshold,
    requestTimeout,
    enforceRequestTimeout = true,
    retry,
    socketFactory = createDefaultSocketFactory(),
    logLevel = LOG_LEVELS.INFO,
    logCreator = consoleLogCreator,
  }: KafkaConfig) {
    this.#logger = createLogger({ level: logLevel, logCreator });
    this.#clusterRetry = retry;
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
    }) =>
      new Cluster({
        logger: this.#logger,
        retry: this.#clusterRetry,
        offsets: this.#offsets,
        socketFactory,
        brokers,
        ssl: resolvedSsl,
        sasl,
        clientId: resolvedClientId,
        connectionTimeout,
        authenticationTimeout,
        reauthenticationThreshold,
        requestTimeout,
        enforceRequestTimeout,
        metadataMaxAge,
        instrumentationEmitter,
        allowAutoTopicCreation,
        maxInFlightRequests,
        isolationLevel,
      });
  }

  /**
   * Create a producer. Idempotent and transactional producers require matching broker support.
   * @see https://kafka.apache.org/43/configuration/producer-configs/
   */
  producer({
    createPartitioner,
    retry,
    metadataMaxAge = DEFAULT_METADATA_MAX_AGE,
    allowAutoTopicCreation,
    idempotent,
    transactionalId,
    transactionTimeout,
    maxInFlightRequests,
    acks,
    compression,
    lingerMs,
    batchSize,
    bufferMemory,
  }: ProducerConfig = {}): Producer {
    const instrumentationEmitter = new InstrumentationEventEmitter();
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
      lingerMs,
      batchSize,
      bufferMemory,
    });
  }

  /**
   * Create a consumer in the given group.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/
   */
  consumer({
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
  }: ConsumerConfig): Consumer {
    const isolationLevel = readUncommitted ? ISOLATION_LEVEL.READ_UNCOMMITTED : ISOLATION_LEVEL.READ_COMMITTED;
    const instrumentationEmitter = new InstrumentationEventEmitter();
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
    });
  }

  /**
   * Create a share-group consumer (KIP-932, Kafka 4.0+).
   * @see https://kafka.apache.org/43/configuration/consumer-configs/
   */
  shareConsumer({
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
  }: ShareConsumerConfig): ShareConsumer {
    const instrumentationEmitter = new InstrumentationEventEmitter();
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
    });
  }

  /**
   * Create an admin client for topics, groups, ACLs, configs, and reassignments.
   * @see https://kafka.apache.org/43/configuration/admin-configs/
   * @see https://kafka.apache.org/43/operations/basic-kafka-operations/
   */
  admin({ retry }: AdminConfig = {}): Admin {
    const instrumentationEmitter = new InstrumentationEventEmitter();
    const cluster = this.#createCluster({
      allowAutoTopicCreation: false,
      instrumentationEmitter,
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
}
