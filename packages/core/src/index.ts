export { Kafka } from './client.js';

import { MemberAssignment, MemberMetadata } from './consumer/assigner-protocol.js';
import { roundRobin } from './consumer/assigners/index.js';
import { DefaultPartitioner, JavaCompatiblePartitioner, LegacyPartitioner } from './producer/partitioners/index.js';

export const PartitionAssigners = Object.freeze({ roundRobin });
export const AssignerProtocol = Object.freeze({ MemberMetadata, MemberAssignment });
export const Partitioners = Object.freeze({
  DefaultPartitioner,
  JavaCompatiblePartitioner,
  LegacyPartitioner,
});

export { LOG_LEVELS as logLevel } from './loggers/index.js';
export { COMPRESSION_TYPES as CompressionTypes, CompressionCodecs } from './protocol/compression/index.js';

export { CONFIG_RESOURCE_TYPES as ConfigResourceTypes } from './protocol/enums/config-resource-types.js';
export { CONFIG_SOURCE as ConfigSource } from './protocol/enums/config-source.js';
export { ACL_RESOURCE_TYPES as AclResourceTypes } from './protocol/enums/acl-resource-types.js';
export { ACL_OPERATION_TYPES as AclOperationTypes } from './protocol/enums/acl-operation-types.js';
export { ACL_PERMISSION_TYPES as AclPermissionTypes } from './protocol/enums/acl-permission-types.js';
export { RESOURCE_PATTERN_TYPES as ResourcePatternTypes } from './protocol/enums/resource-pattern-types.js';

export {
  KafkaJSError,
  KafkaJSNonRetriableError,
  KafkaJSProtocolError,
  KafkaJSOffsetOutOfRange,
  KafkaJSMemberIdRequired,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSConnectionError,
  KafkaJSConnectionClosedError,
  KafkaJSRequestTimeoutError,
  KafkaJSMetadataNotLoaded,
  KafkaJSTopicMetadataNotLoaded,
  KafkaJSStaleTopicMetadataAssignment,
  KafkaJSDeleteGroupsError,
  KafkaJSServerDoesNotSupportApiKey,
  KafkaJSBrokerNotFound,
  KafkaJSPartialMessageError,
  KafkaJSSASLAuthenticationError,
  KafkaJSGroupCoordinatorNotFound,
  KafkaJSNotImplemented,
  KafkaJSTimeout,
  KafkaJSLockTimeout,
  KafkaJSUnsupportedMagicByteInMessageSet,
  KafkaJSDeleteTopicRecordsError,
  KafkaJSInvariantViolation,
  KafkaJSInvalidVarIntError,
  KafkaJSInvalidLongError,
  KafkaJSCreateTopicError,
  KafkaJSAggregateError,
  KafkaJSFetcherRebalanceError,
  KafkaJSNoBrokerAvailableError,
  KafkaJSAlterPartitionReassignmentsError,
} from './errors.js';

export type {
  Admin,
  AdminConfig,
  Assigner,
  AuthenticationProviderArgs,
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
  PartitionerArgs,
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
} from './types/index.js';

export type { AclEntry, AclFilter, TopicConfig, TopicOffset } from './admin/types.js';
export type { CompressionCodec, CompressionCodecFactory } from './protocol/compression/index.js';
