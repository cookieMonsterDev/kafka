import type { ConnectionOptions as TlsConnectionOptions } from 'node:tls';
import type { Admin } from '../admin/types.js';
import type { PartitionMetadata } from '../cluster/index.js';
import type { Batch, Consumer, ConsumerSubscribeTopic, ConsumerSubscribeTopics } from '../consumer/index.js';
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
} from '../consumer/types.js';
import type { LogCreator, LogEntry, LogLevel, Logger } from '../loggers/index.js';
import type { AuthenticationProviderArgs, SaslAuthenticationProvider } from '../network/connection.js';
import type { SocketFactory } from '../network/socket-factory.js';
import type { CompressionType } from '../protocol/compression/index.js';
import type { RecordHeaders } from '../protocol/records/record.js';
import type { Producer, Transaction } from '../producer/index.js';
import type {
  CustomPartitioner,
  Message,
  PartitionerArgs,
  ProducerBatch,
  ProducerRecord,
  RecordMetadata,
  TopicMessages,
} from '../producer/types.js';
import type { RetryOptions } from '../retry/index.js';
import type { ConnectOptions } from '../utils/abort.js';

export type { ConnectOptions };

export type BrokersFunction = () => readonly string[] | Promise<readonly string[]>;

export interface OauthbearerProviderResponse {
  value: string;
}

type SaslMechanismOptionsMap = {
  plain: { username: string; password: string };
  'scram-sha-256': { username: string; password: string };
  'scram-sha-512': { username: string; password: string };
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

export type SaslOptions = SaslMechanismOptions<SaslMechanism>;

export type SaslMechanismProvider = {
  mechanism: string;
  authenticationProvider: (args: AuthenticationProviderArgs) => SaslAuthenticationProvider;
};

export interface KafkaConfig {
  brokers: readonly string[] | BrokersFunction;
  ssl?: TlsConnectionOptions | boolean;
  sasl?: SaslOptions | SaslMechanismProvider;
  clientId?: string;
  connectionTimeout?: number;
  authenticationTimeout?: number;
  reauthenticationThreshold?: number;
  requestTimeout?: number;
  enforceRequestTimeout?: boolean;
  retry?: RetryOptions;
  socketFactory?: SocketFactory;
  logLevel?: LogLevel;
  logCreator?: LogCreator;
}

export interface ProducerConfig {
  createPartitioner?: CustomPartitioner;
  retry?: RetryOptions;
  metadataMaxAge?: number;
  allowAutoTopicCreation?: boolean;
  idempotent?: boolean;
  transactionalId?: string;
  transactionTimeout?: number;
  maxInFlightRequests?: number;
}

export interface ConsumerConfig {
  groupId: string;
  partitionAssigners?: PartitionAssigner[];
  metadataMaxAge?: number;
  sessionTimeout?: number;
  rebalanceTimeout?: number;
  heartbeatInterval?: number;
  maxBytesPerPartition?: number;
  minBytes?: number;
  maxBytes?: number;
  maxWaitTimeInMs?: number;
  retry?: ConsumerRetryOptions;
  allowAutoTopicCreation?: boolean;
  maxInFlightRequests?: number;
  readUncommitted?: boolean;
  rackId?: string;
}

export interface AdminConfig {
  retry?: RetryOptions;
}

export type {
  Admin,
  Assigner,
  AuthenticationProviderArgs,
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
  PartitionerArgs,
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
