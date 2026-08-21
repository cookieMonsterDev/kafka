import type { ConnectionOptions as TlsConnectionOptions } from 'node:tls';
import type { Admin } from '../admin/types';
import type { PartitionMetadata } from '../cluster/index';
import type {
  AutoOffsetReset,
  Batch,
  Consumer,
  ConsumerSubscribeTopic,
  ConsumerSubscribeTopics,
} from '../consumer/index';
import type {
  Assigner,
  ConsumerRetryOptions,
  ConsumerRunConfig,
  EachBatchHandler,
  EachBatchPayload,
  EachMessageHandler,
  EachMessagePayload,
  KafkaMessage,
  PartitionAssigner,
  TopicPartitionOffset,
  TopicPartitionOffsetAndMetadata,
  TopicPartitions,
} from '../consumer/types';
import type { LogCreator, LogEntry, LogLevel, Logger } from '../loggers/index';
import type { AuthenticationProviderArgs, SaslAuthenticationProvider } from '../network/connection';
import type { SocketFactory } from '../network/socket-factory';
import type { CompressionType } from '../protocol/compression/index';
import type { RecordHeaders } from '../protocol/records/record';
import type { Producer, Transaction } from '../producer/index';
import type {
  CustomPartitioner,
  Message,
  Partitioner,
  PartitionerArgs,
  PartitionerBatchArgs,
  ProducerBatch,
  ProducerRecord,
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
  /** Socket connect timeout in milliseconds. */
  connectionTimeout?: number;
  /** SASL handshake timeout in milliseconds. */
  authenticationTimeout?: number;
  /** Reauthenticate this many milliseconds before the broker session expires. */
  reauthenticationThreshold?: number;
  /** Per-request timeout in milliseconds. */
  requestTimeout?: number;
  /** When false, in-flight requests are not timed out by the client. */
  enforceRequestTimeout?: boolean;
  retry?: RetryOptions;
  socketFactory?: SocketFactory;
  logLevel?: LogLevel;
  logCreator?: LogCreator;
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
}

/**
 * Options for {@link Kafka.consumer}.
 * @see https://kafka.apache.org/43/configuration/consumer-configs/
 */
export interface ConsumerConfig {
  /**
   * Consumer group id. Members that share this id partition assigned topics among themselves.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#group.id
   */
  groupId: string;
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
}

/**
 * Options for {@link Kafka.admin}.
 * @see https://kafka.apache.org/43/configuration/admin-configs/
 */
export interface AdminConfig {
  retry?: RetryOptions;
}

export type {
  Admin,
  Assigner,
  AuthenticationProviderArgs,
  AutoOffsetReset,
  Batch,
  CompressionType,
  Consumer,
  ConsumerRetryOptions,
  ConsumerRunConfig,
  ConsumerSubscribeTopic,
  ConsumerSubscribeTopics,
  CustomPartitioner,
  EachBatchHandler,
  EachBatchPayload,
  EachMessageHandler,
  EachMessagePayload,
  KafkaMessage,
  LogCreator,
  LogEntry,
  LogLevel,
  Logger,
  Message,
  PartitionAssigner,
  PartitionMetadata,
  Partitioner,
  PartitionerArgs,
  PartitionerBatchArgs,
  Producer,
  ProducerBatch,
  ProducerRecord,
  RecordHeaders,
  RecordMetadata,
  RetryOptions,
  SaslAuthenticationProvider,
  SocketFactory,
  TopicMessages,
  TopicPartitionOffset,
  TopicPartitionOffsetAndMetadata,
  TopicPartitions,
  Transaction,
};
