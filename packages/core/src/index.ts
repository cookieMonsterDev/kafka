/**
 * TypeScript Kafka client for Kafka 0.10+.
 *
 * @see https://kafka.apache.org/43/getting-started/introduction/
 * @see https://kafka.apache.org/43/design/design/
 * @see https://kafka.apache.org/43/design/protocol/
 */
export { Kafka } from './client';

import { MemberAssignment, MemberMetadata } from './consumer/assigner-protocol';
import { cooperativeSticky, range, roundRobin, sticky } from './consumer/assigners/index';
import {
  DefaultPartitioner,
  JavaCompatiblePartitioner,
  LegacyPartitioner,
  StickyPartitioner,
} from './producer/partitioners/index';

export const PartitionAssigners = Object.freeze({ roundRobin, range, sticky, cooperativeSticky });
export const AssignerProtocol = Object.freeze({ MemberMetadata, MemberAssignment });
export const Partitioners = Object.freeze({
  DefaultPartitioner,
  JavaCompatiblePartitioner,
  LegacyPartitioner,
  StickyPartitioner,
});

export { LOG_LEVELS as logLevel } from './loggers/index';
export { COMPRESSION_TYPES as CompressionTypes, CompressionCodecs } from './protocol/compression/index';

export { CONFIG_RESOURCE_TYPES as ConfigResourceTypes } from './protocol/enums/config-resource-types';
export { INCREMENTAL_ALTER_CONFIGS_OPERATIONS as ConfigOperations } from './protocol/enums/incremental-alter-configs-operations';
export { CONFIG_SOURCE as ConfigSource } from './protocol/enums/config-source';
export { CONFIG_TYPE as ConfigType } from './protocol/enums/config-type';
export { ACL_RESOURCE_TYPES as AclResourceTypes } from './protocol/enums/acl-resource-types';
export { ACL_OPERATION_TYPES as AclOperationTypes } from './protocol/enums/acl-operation-types';
export { ACL_PERMISSION_TYPES as AclPermissionTypes } from './protocol/enums/acl-permission-types';
export { RESOURCE_PATTERN_TYPES as ResourcePatternTypes } from './protocol/enums/resource-pattern-types';
export { SCRAM_MECHANISMS as ScramMechanisms } from './protocol/enums/scram-mechanisms';
export { FEATURE_UPDATE_UPGRADE_TYPES as FeatureUpdateUpgradeTypes } from './admin/types';

export {
  KafkaError,
  KafkaNonRetriableError,
  KafkaProtocolError,
  KafkaOffsetOutOfRange,
  KafkaMemberIdRequired,
  KafkaNumberOfRetriesExceeded,
  KafkaConnectionError,
  KafkaConnectionClosedError,
  KafkaRequestTimeoutError,
  KafkaMetadataNotLoaded,
  KafkaTopicMetadataNotLoaded,
  KafkaStaleTopicMetadataAssignment,
  KafkaDeleteGroupsError,
  KafkaServerDoesNotSupportApiKey,
  KafkaBrokerNotFound,
  KafkaPartialMessageError,
  KafkaSASLAuthenticationError,
  KafkaGroupCoordinatorNotFound,
  KafkaNotImplemented,
  KafkaTimeout,
  KafkaLockTimeout,
  KafkaUnsupportedMagicByteInMessageSet,
  KafkaDeleteTopicRecordsError,
  KafkaInvariantViolation,
  KafkaInvalidVarIntError,
  KafkaInvalidLongError,
  KafkaCreateTopicError,
  KafkaAggregateError,
  KafkaFetcherRebalanceError,
  KafkaNoBrokerAvailableError,
  KafkaAlterPartitionReassignmentsError,
  KafkaUpdateFeaturesError,
} from './errors';

export type {
  Admin,
  AdminConfig,
  Assigner,
  AuthenticationProviderArgs,
  AutoOffsetReset,
  Batch,
  BrokersFunction,
  CompressionType,
  ConnectOptions,
  Consumer,
  ConsumerConfig,
  ConsumerRetryOptions,
  ConsumerRunConfig,
  ConsumerSubscribeTopic,
  ConsumerSubscribeTopics,
  CustomPartitioner,
  EachBatchHandler,
  EachBatchPayload,
  EachMessageHandler,
  EachMessagePayload,
  GroupProtocol,
  KafkaConfig,
  KafkaMessage,
  LogCreator,
  LogEntry,
  LogLevel,
  Logger,
  Message,
  OauthbearerProviderResponse,
  PartitionAssigner,
  PartitionMetadata,
  Partitioner,
  PartitionerArgs,
  PartitionerBatchArgs,
  Producer,
  ProducerBatch,
  ProducerConfig,
  ProducerRecord,
  RecordHeaders,
  RecordMetadata,
  RetryOptions,
  SaslAuthenticationProvider,
  SaslMechanism,
  SaslMechanismProvider,
  SaslOptions,
  SocketFactory,
  TopicMessages,
  TopicPartitionOffset,
  TopicPartitionOffsetAndMetadata,
  TopicPartitions,
  Transaction,
} from './types/index';

export type {
  ActiveProducerState,
  AclEntry,
  AclFilter,
  DescribeProducersOptions,
  FeatureUpdate,
  FeatureUpdateUpgradeType,
  ListTransactionsOptions,
  PartitionProducerState,
  TopicConfig,
  TopicOffset,
  TransactionDescription,
  TransactionListing,
  TransactionTopic,
  UpdateFeaturesOptions,
  UpdateFeaturesResult,
} from './admin/types';
export type { CompressionCodec, CompressionCodecFactory } from './protocol/compression/index';
