import type { ConnectionOptions as TlsConnectionOptions } from 'node:tls';
import type { Admin } from '../admin/types';
import type { MetadataRecovery } from '../cluster/broker-pool';
import type { PartitionMetadata, TopicPartitionInfo } from '../cluster/index';
import type {
  AutoOffsetReset,
  Batch,
  Consumer,
  ConsumerSubscribeTopic,
  ConsumerSubscribeTopics,
} from '../consumer/index';
import type { KafkaMetrics } from '../instrumentation/metrics';
import type {
  Assigner,
  ConsumerHooks,
  ConsumerRetryOptions,
  ConsumerRunConfig,
  EachBatchHandler,
  EachBatchPayload,
  EachMessageHandler,
  EachMessagePayload,
  KafkaMessage,
  OnCommitEvent,
  OnCommitHook,
  OnConsumeEvent,
  OnConsumeHook,
  PartitionAssigner,
  RebalanceListener,
  TopicPartition,
  TopicPartitionOffset,
  TopicPartitionOffsetAndMetadata,
  TopicPartitions,
} from '../consumer/types';
import type { LogCreator, LogEntry, LogLevel, Logger } from '../loggers/index';
import type { AuthenticationProviderArgs, SaslAuthenticationProvider } from '../network/connection';
import type { ClientDnsLookup } from '../network/dns-lookup';
import type { SocketFactory } from '../network/socket-factory';
import type { CompressionType } from '../protocol/compression/index';
import type { ShareAcquireMode } from '../protocol/requests/share-fetch/index';
import type { GssTokenChallenge, GssTokenProvider, GssTokenStep } from '../protocol/sasl/gssapi';
import type { RecordHeaders } from '../protocol/records/record';
import type { Producer, Transaction } from '../producer/index';
import type { NodeLatencyReader } from '../producer/node-latency-tracker';
import type {
  CustomPartitioner,
  Message,
  Partitioner,
  PartitionerArgs,
  PartitionerBatchArgs,
  ProducerAckHook,
  ProducerAckHookEvent,
  ProducerBatch,
  ProducerHooks,
  ProducerRecord,
  ProducerSendHook,
  ProducerSendHookEvent,
  RecordMetadata,
  TopicMessages,
} from '../producer/types';
import type { RetryOptions } from '../retry/index';
import type { ConnectOptions } from '../utils/abort';

export type { ConnectOptions };

/** Resolves bootstrap brokers at connect time as `host:port` strings. */
export type BrokersFunction = () => readonly string[] | Promise<readonly string[]>;

/** Token returned by an OAUTHBEARER provider. @see https://kafka.apache.org/43/security/authentication-using-sasl/ */
export interface OauthbearerProviderResponse {
  value: string;
}

/**
 * SASL/SCRAM password login, or delegation-token login (KIP-48). Token auth
 * reuses SCRAM-SHA-256 / SCRAM-SHA-512 on the wire: `tokenId` is the username,
 * `tokenHmac` is the password (Buffer values are sent as standard base64), and
 * the client-first message includes `tokenauth=true`.
 *
 * @see https://kafka.apache.org/43/security/authentication-using-sasl/
 */
export type ScramSaslOptions = { username: string; password: string } | { tokenId: string; tokenHmac: Buffer | string };

type SaslMechanismOptionsMap = {
  plain: { username: string; password: string };
  'scram-sha-256': ScramSaslOptions;
  'scram-sha-512': ScramSaslOptions;
  aws: {
    authorizationIdentity: string;
    accessKeyId: string;
    secretAccessKey: string;
    sessionToken?: string;
  };
  oauthbearer: { oauthBearerProvider: () => Promise<OauthbearerProviderResponse> };
  /**
   * SASL/GSSAPI (Kerberos). Handshake name is `GSSAPI`. Supply `gssProvider` or
   * install the optional `kerberos` package. `serviceName` defaults to `kafka`.
   */
  gssapi: {
    serviceName?: string;
    principal?: string;
    keytab?: string;
    krb5?: string;
    authorizationIdentity?: string;
    gssProvider?: GssTokenProvider;
  };
};

export type SaslMechanism = keyof SaslMechanismOptionsMap;

type SaslMechanismOptions<T> = T extends SaslMechanism ? { mechanism: T } & SaslMechanismOptionsMap[T] : never;

/** Built-in SASL options. @see https://kafka.apache.org/43/security/authentication-using-sasl/ */
export type SaslOptions = SaslMechanismOptions<SaslMechanism>;

/** Custom SASL mechanism with a user-supplied authenticator. */
export type SaslMechanismProvider = {
  mechanism: string;
  authenticationProvider: (args: AuthenticationProviderArgs) => SaslAuthenticationProvider;
};

/**
 * Shared client options used by {@link Kafka.producer}, {@link Kafka.consumer}, and {@link Kafka.admin}.
 *
 * @see https://kafka.apache.org/43/getting-started/introduction/
 * @see https://kafka.apache.org/43/security/security-overview/
 */
export interface KafkaConfig {
  /** Bootstrap servers as `host:port`, or a function that returns them. */
  brokers: readonly string[] | BrokersFunction;
  /**
   * Enable TLS. `true` uses default Node TLS options; an object is passed to `tls.connect`.
   * @see https://kafka.apache.org/43/security/encryption-and-authentication-using-ssl/
   */
  ssl?: TlsConnectionOptions | boolean;
  /**
   * SASL credentials or a custom mechanism provider. SCRAM mechanisms accept
   * either `username`/`password` or delegation-token `tokenId`/`tokenHmac`.
   * @see https://kafka.apache.org/43/security/authentication-using-sasl/
   */
  sasl?: SaslOptions | SaslMechanismProvider;
  /**
   * Logical client identifier sent in the request header.
   * @see https://kafka.apache.org/43/configuration/producer-configs/#client.id
   */
  clientId?: string;
  /** Socket connect timeout in milliseconds. Also the initial TLS/TCP setup timeout. */
  connectionTimeout?: number;
  /**
   * Close idle broker sockets after this many milliseconds with no send/receive and no in-flight
   * requests. Default 540_000 (9 minutes). `0` disables idle close.
   * [connections.max.idle.ms](https://kafka.apache.org/43/configuration/producer-configs/#connections.max.idle.ms)
   */
  connectionsMaxIdleMs?: number;
  /**
   * Cap, in ms, for exponential growth of the connect/TLS handshake timeout after consecutive
   * failures. The initial timeout is {@link KafkaConfig.connectionTimeout}. Default 30_000.
   * [socket.connection.setup.timeout.max.ms](https://kafka.apache.org/43/configuration/producer-configs/#socket.connection.setup.timeout.max.ms)
   */
  socketConnectionSetupTimeoutMaxMs?: number;
  /**
   * How bootstrap hostnames are resolved. Default `'useAllDnsIps'` (every A/AAAA, happy-eyeballs
   * when both families exist). `'canonicalBootstrap'` follows CNAME/PTR so GSSAPI sees the FQDN.
   * [client.dns.lookup](https://kafka.apache.org/43/configuration/producer-configs/#client.dns.lookup)
   */
  clientDnsLookup?: ClientDnsLookup;
  /**
   * Initial wait before reconnecting a dropped socket. Default 50. `0` disables.
   * [reconnect.backoff.ms](https://kafka.apache.org/43/configuration/producer-configs/#reconnect.backoff.ms)
   */
  reconnectBackoffMs?: number;
  /**
   * Cap for reconnect backoff. Default 1000.
   * [reconnect.backoff.max.ms](https://kafka.apache.org/43/configuration/producer-configs/#reconnect.backoff.max.ms)
   */
  reconnectBackoffMaxMs?: number;
  /** SASL handshake timeout in milliseconds. */
  authenticationTimeout?: number;
  /** Reauthenticate this many milliseconds before the broker session expires. */
  reauthenticationThreshold?: number;
  /** Per-request timeout in milliseconds. */
  requestTimeout?: number;
  /** When false, in-flight requests are not timed out by the client. */
  enforceRequestTimeout?: boolean;
  /**
   * KIP-1102: on `REBOOTSTRAP_REQUIRED` or an exhausted/unreachable broker set, `'rebootstrap'`
   * (the default) drops discovered metadata and reconnects to the original bootstrap broker
   * list; `'none'` keeps retrying the brokers already known to the client.
   */
  metadataRecovery?: MetadataRecovery;
  retry?: RetryOptions;
  socketFactory?: SocketFactory;
  logLevel?: LogLevel;
  logCreator?: LogCreator;
  /**
   * Client-side metrics. Off by default. `true` uses the global `@opentelemetry/api` meter
   * (optional peer). Pass `{ meter }` to supply any OpenTelemetry-compatible `Meter`.
   */
  metrics?: KafkaMetrics;
  /**
   * KIP-714: after connect, subscribe and push client metrics to the broker when it
   * advertises GetTelemetrySubscriptions (Kafka 3.5+). Default true; the pusher disables
   * itself if the API is missing. Set false to skip the RPCs entirely.
   * [enable.metrics.push](https://kafka.apache.org/43/configuration/producer-configs/#enable.metrics.push)
   */
  enableMetricsPush?: boolean;
}

/**
 * Options for {@link Kafka.producer}.
 * @see https://kafka.apache.org/43/configuration/producer-configs/
 */
export interface ProducerConfig {
  createPartitioner?: CustomPartitioner;
  retry?: RetryOptions;
  /** How long cached topic metadata is considered fresh, in milliseconds. */
  metadataMaxAge?: number;
  /**
   * Create the topic when a produce targets one that does not exist.
   * @see https://kafka.apache.org/43/configuration/broker-configs/#auto.create.topics.enable
   */
  allowAutoTopicCreation?: boolean;
  /**
   * Assign producer ids and sequence numbers so the broker can deduplicate retries.
   * Defaults to false. Java 3.0+ defaults `enable.idempotence` to true.
   * @see https://kafka.apache.org/43/configuration/producer-configs/#enable.idempotence
   */
  idempotent?: boolean;
  /**
   * Transactional id that fences zombie producers and enables {@link Producer.transaction}.
   * @see https://kafka.apache.org/43/configuration/producer-configs/#transactional.id
   */
  transactionalId?: string;
  /**
   * Transaction timeout in milliseconds.
   * @see https://kafka.apache.org/43/configuration/producer-configs/#transaction.timeout.ms
   */
  transactionTimeout?: number;
  maxInFlightRequests?: number;
  /**
   * Default acks for send/sendBatch when the call omits acks. `-1` = all ISR.
   * @see https://kafka.apache.org/43/configuration/producer-configs/#acks
   */
  acks?: number;
  /**
   * Default compression for send/sendBatch when the call omits compression.
   * @see https://kafka.apache.org/43/configuration/producer-configs/#compression.type
   */
  compression?: CompressionType;
  /**
   * Default compression level for send/sendBatch when the call omits compressionLevel, passed
   * to the active codec when it honors one. GZIP maps it straight to zlib's `level` (0-9). ZSTD
   * maps it to `zlib.constants.ZSTD_c_compressionLevel` (roughly 1-22). Snappy and LZ4 have no
   * compression-level concept in this client's codecs and ignore it.
   * @see https://kafka.apache.org/43/configuration/producer-configs/#compression.gzip.level
   */
  compressionLevel?: number;
  /**
   * Delay in ms to wait for more records before sending a Produce request.
   * Default 0 (send immediately). Java 4.0+ defaults to 5.
   * @see https://kafka.apache.org/43/configuration/producer-configs/#linger.ms
   */
  lingerMs?: number;
  /**
   * Soft cap on buffered record bytes before a Produce is sent (with lingerMs).
   * Ignored when lingerMs is 0. Unset or 0 means do not batch by size.
   * Java default is 16384.
   * @see https://kafka.apache.org/43/configuration/producer-configs/#batch.size
   */
  batchSize?: number;
  /**
   * Max bytes of linger-buffered records waiting to be sent. `send()` waits until
   * a flush frees space, or rejects with `KafkaTimeout` after the send timeout.
   * Unset or 0 means unlimited. Java `buffer.memory` is 32 MiB; this client's
   * constructor stays unlimited unless set (see `throughputPreset()`).
   * @see https://kafka.apache.org/43/configuration/producer-configs/#buffer.memory
   */
  bufferMemory?: number;
  /**
   * End-to-end deadline for one `send`/`sendBatch` call, covering `lingerMs`, any
   * `bufferMemory` wait, and every retry attempt together - not any single RPC. Once it
   * elapses, the call rejects with `KafkaDeliveryTimeoutError` regardless of retries
   * remaining; the in-flight attempt, if any, is not cancelled. Default 120_000; 0 (or
   * below) disables the deadline. Keep it comfortably above `lingerMs` plus the per-call
   * `timeout` (or the broker could still be waiting on acks when this fires) and above
   * `retry.maxRetryTime` times however many retries you expect to need.
   * @see https://kafka.apache.org/43/configuration/producer-configs/#delivery.timeout.ms
   */
  deliveryTimeoutMs?: number;
  /**
   * Cap, in bytes, on the uncompressed records of one Produce request. A single record over the
   * cap rejects immediately at `send`/`sendBatch` call time with `KafkaMessageTooLargeError`,
   * before it ever occupies a linger slot; a linger-buffered batch that would otherwise combine
   * past the cap is flushed as multiple requests instead, none over the cap. Default 1_048_576
   * (1 MiB), matching Java's default. Distinct from the broker's `MESSAGE_TOO_LARGE` protocol
   * error, which only fires after the broker has accepted bytes on the wire.
   * @see https://kafka.apache.org/43/configuration/producer-configs/#max.request.size
   */
  maxRequestSize?: number;
  /**
   * Ordered async hooks around the send path, not an interceptor SPI: `onSend` fires before a
   * `send()`/`sendBatch()` call is dispatched, `onAck` after it settles. Each array runs in
   * registration order; a throwing hook is caught and logged, never failing the send.
   */
  hooks?: ProducerHooks;
}

/**
 * Options for {@link Kafka.consumer}.
 * @see https://kafka.apache.org/43/configuration/consumer-configs/
 */
export interface ConsumerConfig {
  /**
   * Consumer group id. Members that share this id partition assigned topics among themselves.
   * Required to use {@link Consumer.subscribe}. Optional for {@link Consumer.assign}, which
   * fetches without group membership; set it there only if you plan to call
   * {@link Consumer.commitOffsets}.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#group.id
   */
  groupId?: string;
  partitionAssigners?: PartitionAssigner[];
  metadataMaxAge?: number;
  /**
   * Session timeout used by the group coordinator to detect failed members.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#session.timeout.ms
   */
  sessionTimeout?: number;
  /**
   * Maximum time for a rebalance to complete.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#max.poll.interval.ms
   */
  rebalanceTimeout?: number;
  /**
   * How often to send heartbeats to the group coordinator.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#heartbeat.interval.ms
   */
  heartbeatInterval?: number;
  /**
   * Cap on bytes returned for a single partition in a Fetch.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#max.partition.fetch.bytes
   */
  maxBytesPerPartition?: number;
  /**
   * Wait for at least this many bytes before returning a Fetch (or `maxWaitTimeInMs`).
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#fetch.min.bytes
   */
  minBytes?: number;
  /**
   * Maximum bytes the broker should return for a Fetch.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#fetch.max.bytes
   */
  maxBytes?: number;
  /**
   * How long the broker may wait to accumulate `minBytes`.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#fetch.max.wait.ms
   */
  maxWaitTimeInMs?: number;
  retry?: ConsumerRetryOptions;
  allowAutoTopicCreation?: boolean;
  maxInFlightRequests?: number;
  /**
   * When true, use `read_uncommitted` isolation (aborted transactional records are visible).
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#isolation.level
   */
  readUncommitted?: boolean;
  /**
   * Consumer rack for fetch-from-closest-replica (KIP-392).
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#client.rack
   */
  rackId?: string;
  /**
   * Static membership id (KIP-345). When set, the broker can replace this member on restart
   * without bouncing the rest of the group.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#group.instance.id
   */
  groupInstanceId?: string;
  /**
   * Default offset reset policy when a subscription omits `autoOffsetReset`.
   * Per-subscription `subscribe({ autoOffsetReset })` overrides this.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#auto.offset.reset
   */
  autoOffsetReset?: AutoOffsetReset;
  /**
   * Group membership protocol. `'classic'` (default) uses JoinGroup/SyncGroup.
   * `'consumer'` opts into KIP-848 ConsumerGroupHeartbeat (Kafka 4.0+). Java name:
   * `group.protocol`. Classic remains the default.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#group.protocol
   */
  groupProtocol?: GroupProtocol;
  /**
   * Server-side partition assignor to request under the KIP-848 consumer protocol. Only
   * meaningful when `groupProtocol` is `'consumer'`; ignored (logged at debug level) otherwise,
   * the same as `sessionTimeout`, `heartbeatInterval`, and `partitionAssigners` are unused under
   * `groupProtocol: 'consumer'`. Broker property: `group.remote.assignor`.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#group.remote.assignor
   */
  groupRemoteAssignor?: GroupRemoteAssignor;
  /**
   * Ordered async hooks around the consume/commit path, not an interceptor SPI: `onConsume`
   * fires before `eachMessage`/`eachBatch` runs, `onCommit` after an offset-commit attempt
   * settles (auto-commit or manual `commitOffsets`). Each array runs in registration order; a
   * throwing hook is caught and logged, never failing consumption or the commit.
   */
  hooks?: ConsumerHooks;
  /**
   * Verify each fetched record batch's CRC (RecordBatch v2 CRC-32C, or the legacy MessageSet
   * CRC-32) and throw {@link KafkaCorruptRecordError} on mismatch. Default `true`. Set `false`
   * to skip the check for extreme throughput - corrupted bytes on the wire (a bad disk, a buggy
   * proxy, transport bit-flips) then go undetected instead of failing loudly.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#check.crcs
   */
  checkCrcs?: boolean;
}

export type GroupProtocol = 'classic' | 'consumer';

/** Broker-side partition assignor requested via `groupRemoteAssignor` (KIP-848, `group.remote.assignor`). */
export type GroupRemoteAssignor = 'uniform' | 'range';

/**
 * Options for {@link Kafka.shareConsumer} (KIP-932 share groups, Kafka 4.0+).
 * @see https://kafka.apache.org/43/configuration/consumer-configs/
 */
export interface ShareConsumerConfig {
  groupId: string;
  heartbeatInterval?: number;
  maxWaitTimeInMs?: number;
  minBytes?: number;
  maxBytes?: number;
  maxRecords?: number;
  batchSize?: number;
  /**
   * ShareFetch v2 acquire mode (KIP-1206, Kafka 4.2+). `0` batches for throughput;
   * `1` stops at `maxRecords`. Negotiates down to v1 on 4.1 brokers.
   */
  shareAcquireMode?: ShareAcquireMode;
  rackId?: string;
  retry?: ConsumerRetryOptions;
  metadataMaxAge?: number;
  allowAutoTopicCreation?: boolean;
  maxInFlightRequests?: number;
}

/**
 * Options for {@link Kafka.admin}.
 * @see https://kafka.apache.org/43/configuration/admin-configs/
 */
export interface AdminConfig {
  retry?: RetryOptions;
  /**
   * KIP-919: talk to the KRaft controller quorum without a broker bootstrap list.
   * Mutually exclusive with discovering brokers for this admin instance; producer and consumer
   * still use {@link KafkaConfig.brokers}. Requires DescribeCluster v1 (Kafka 3.7+).
   * @see https://kafka.apache.org/43/configuration/admin-configs/#bootstrap.controllers
   */
  bootstrapControllers?: readonly string[] | BrokersFunction;
}

export type {
  Admin,
  Assigner,
  AuthenticationProviderArgs,
  AutoOffsetReset,
  Batch,
  CompressionType,
  Consumer,
  ConsumerHooks,
  ConsumerRetryOptions,
  ConsumerRunConfig,
  ConsumerSubscribeTopic,
  ConsumerSubscribeTopics,
  CustomPartitioner,
  EachBatchHandler,
  EachBatchPayload,
  EachMessageHandler,
  EachMessagePayload,
  GssTokenChallenge,
  GssTokenProvider,
  GssTokenStep,
  KafkaMessage,
  LogCreator,
  LogEntry,
  LogLevel,
  Logger,
  Message,
  NodeLatencyReader,
  OnCommitEvent,
  OnCommitHook,
  OnConsumeEvent,
  OnConsumeHook,
  PartitionAssigner,
  PartitionMetadata,
  TopicPartitionInfo,
  Partitioner,
  PartitionerArgs,
  PartitionerBatchArgs,
  Producer,
  ProducerAckHook,
  ProducerAckHookEvent,
  ProducerBatch,
  ProducerHooks,
  ProducerRecord,
  ProducerSendHook,
  ProducerSendHookEvent,
  RebalanceListener,
  RecordHeaders,
  RecordMetadata,
  SaslAuthenticationProvider,
  SocketFactory,
  TopicMessages,
  TopicPartition,
  TopicPartitionOffset,
  TopicPartitionOffsetAndMetadata,
  TopicPartitions,
  Transaction,
};

export type { RetryOptions } from '../retry/index';
export type { ClientDnsLookup } from '../network/dns-lookup';

export type {
  EachShareBatchHandler,
  EachShareBatchPayload,
  ShareConsumer,
  ShareConsumerRunConfig,
  ShareConsumerSubscribeTopics,
} from '../share-consumer/index';
export type { ShareAcknowledgeType } from '../share-consumer/acknowledge-types';
export type { ShareAcquireMode } from '../protocol/requests/share-fetch/index';
