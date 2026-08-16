import type { ConnectionOptions as TlsConnectionOptions } from 'node:tls';
import { createAdmin } from './admin/index.js';
import type { Admin } from './admin/types.js';
import { Cluster, type CommittedOffsetsByGroup } from './cluster/index.js';
import { createConsumer, type Consumer } from './consumer/index.js';
import { InstrumentationEventEmitter } from './instrumentation/emitter.js';
import { consoleLogCreator } from './loggers/console.js';
import { createLogger, LOG_LEVELS, type Logger } from './loggers/index.js';
import { createDefaultSocketFactory } from './network/socket-factory.js';
import { createProducer, type Producer } from './producer/index.js';
import { ISOLATION_LEVEL, type IsolationLevel } from './protocol/enums/isolation-level.js';
import type { AdminConfig, ConsumerConfig, KafkaConfig, ProducerConfig } from './types/index.js';
import { once } from './utils/once.js';

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
 * The public client: one shared logger and offset map, plus `producer()` / `consumer()` / `admin()`
 * factories that each get their own cluster and instrumentation emitter.
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
      if (process.env.KAFKAJS_NO_PARTITIONER_WARNING == null) {
        logger.warn(
          'The default partitioner changed. To retain the previous routing, create the producer with "createPartitioner: Partitioners.LegacyPartitioner". Silence this warning by setting the environment variable "KAFKAJS_NO_PARTITIONER_WARNING=1"',
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

  producer({
    createPartitioner,
    retry,
    metadataMaxAge = DEFAULT_METADATA_MAX_AGE,
    allowAutoTopicCreation,
    idempotent,
    transactionalId,
    transactionTimeout,
    maxInFlightRequests,
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
    });
  }

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
    });
  }

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
