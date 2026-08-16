import type { Cluster, PartitionMetadata } from '../cluster/index';
import type { InstrumentationEventEmitter, RemoveInstrumentationEventListener } from '../instrumentation/emitter';
import type { InstrumentationEvent } from '../instrumentation/event';
import type { Logger } from '../loggers/index';
import type { AclOperationType } from '../protocol/enums/acl-operation-types';
import type { AclPermissionType } from '../protocol/enums/acl-permission-types';
import type { AclResourceType } from '../protocol/enums/acl-resource-types';
import type { ConfigResourceType } from '../protocol/enums/config-resource-types';
import type { ResourcePatternType } from '../protocol/enums/resource-pattern-types';
import type { AlterClientQuotasResponseV1Body } from '../protocol/requests/alter-client-quotas/v1/response';
import type { AlterConfigsResponseV1Body } from '../protocol/requests/alter-configs/v1/response';
import type { AlterReplicaLogDirsResponseV2Body } from '../protocol/requests/alter-replica-log-dirs/v2/response';
import type { DescribeAclsResponseV1Body } from '../protocol/requests/describe-acls/v1/response';
import type { DescribeClientQuotasResponseV1Body } from '../protocol/requests/describe-client-quotas/v1/response';
import type { DescribeLogDirsResponseV2Body } from '../protocol/requests/describe-log-dirs/v2/response';
import type { DescribeConfigsResponseV2Body } from '../protocol/requests/describe-configs/v2/response';
import type { DescribeGroupsResponseV2Body } from '../protocol/requests/describe-groups/v2/response';
import type { DeleteAclsResponseV1Body } from '../protocol/requests/delete-acls/v1/response';
import type { DeleteGroupsResult } from '../protocol/requests/delete-groups/v0/response';
import type { ElectLeadersResponseV0Body } from '../protocol/requests/elect-leaders/v0/response';
import type { IncrementalAlterConfigsResponseV1Body } from '../protocol/requests/incremental-alter-configs/v1/response';
import type { ListGroupsResponseV2Body } from '../protocol/requests/list-groups/v2/response';
import type { ListPartitionReassignmentsResponseV0Body } from '../protocol/requests/list-partition-reassignments/v0/response';
import type { OffsetDeleteResponseV0Body } from '../protocol/requests/offset-delete/v0/response';
import type { AlterUserScramCredentialsResponseV0Body } from '../protocol/requests/alter-user-scram-credentials/v0/response';
import type { DescribeUserScramCredentialsResponseV0Body } from '../protocol/requests/describe-user-scram-credentials/v0/response';
import type { RetryOptions } from '../retry/index';
import type { ConnectOptions } from '../utils/abort';
import type { AdminEventName } from './instrumentation-events';
import { events } from './instrumentation-events';

export type OffsetInput = bigint | number | string;

export interface ReplicaAssignment {
  partition: number;
  replicas: number[];
}

export interface ResourceConfigEntry {
  name: string;
  value: string;
}

/**
 * Topic to create via {@link Admin.createTopics}.
 * @see https://kafka.apache.org/43/configuration/topic-configs/
 * @see https://kafka.apache.org/43/operations/basic-kafka-operations/
 */
export interface TopicConfig {
  topic: string;
  numPartitions?: number;
  replicationFactor?: number;
  replicaAssignment?: ReplicaAssignment[];
  configEntries?: ResourceConfigEntry[];
}

export interface TopicPartitionConfig {
  topic: string;
  count: number;
  assignments?: number[][];
}

export interface TopicMetadata {
  name: string;
  partitions: PartitionMetadata[];
}

export interface SeekEntry {
  partition: number;
  offset: bigint;
}

export interface SeekInput {
  partition: number;
  offset: OffsetInput;
}

export interface TopicOffset {
  partition: number;
  offset: bigint;
  high: bigint;
  low: bigint;
}

export interface FetchOffsetsPartition {
  partition: number;
  offset: bigint;
  metadata: string | null;
}

export interface ResourceConfigQuery {
  type: ConfigResourceType;
  name: string;
  configNames?: string[];
}

export interface ResourceConfig {
  type: ConfigResourceType;
  name: string;
  configEntries: ResourceConfigEntry[];
}

export interface IncrementalResourceConfigEntry {
  name: string;
  value: string | null;
  operation: number;
}

export interface IncrementalResourceConfig {
  type: ConfigResourceType;
  name: string;
  configs: IncrementalResourceConfigEntry[];
}

/**
 * ACL identity and permission.
 * @see https://kafka.apache.org/43/security/authorization-and-acls/
 */
export interface Acl {
  principal: string;
  host: string;
  operation: AclOperationType;
  permissionType: AclPermissionType;
}

export interface AclResource {
  resourceType: AclResourceType;
  resourceName: string;
  resourcePatternType: ResourcePatternType;
}

export type AclEntry = Acl & AclResource;

export interface AclFilter {
  resourceType: AclResourceType;
  resourceName?: string;
  resourcePatternType: ResourcePatternType;
  principal?: string;
  host?: string;
  operation: AclOperationType;
  permissionType: AclPermissionType;
}

export interface PartitionReassignment {
  topic: string;
  partitionAssignment: ReplicaAssignment[];
}

export interface TopicPartitions {
  topic: string;
  partitions: number[];
}

export interface ClusterDescription {
  brokers: { nodeId: number; host: string; port: number }[];
  controller: number | null;
  clusterId: string | null;
}

export interface AdminOptions {
  cluster: Cluster;
  logger: Logger;
  retry?: RetryOptions;
  instrumentationEmitter?: InstrumentationEventEmitter | null;
}

/**
 * Admin client returned by {@link Kafka.admin}.
 * @see https://kafka.apache.org/43/operations/basic-kafka-operations/
 * @see https://kafka.apache.org/43/configuration/admin-configs/
 */
export interface Admin {
  connect: (options?: ConnectOptions) => Promise<void>;
  disconnect: (options?: ConnectOptions) => Promise<void>;
  listTopics: () => Promise<string[]>;
  createTopics: (options: {
    topics: TopicConfig[];
    validateOnly?: boolean;
    timeout?: number;
    waitForLeaders?: boolean;
  }) => Promise<boolean>;
  deleteTopics: (options: { topics: string[]; timeout?: number }) => Promise<void>;
  createPartitions: (options: {
    topicPartitions: TopicPartitionConfig[];
    validateOnly?: boolean;
    timeout?: number;
  }) => Promise<void>;
  fetchTopicMetadata: (options?: { topics?: string[] }) => Promise<{ topics: TopicMetadata[] }>;
  describeCluster: () => Promise<ClusterDescription>;
  deleteTopicRecords: (options: { topic: string; partitions: SeekInput[] }) => Promise<void>;
  fetchOffsets: (options: {
    groupId: string;
    topics?: string[];
    resolveOffsets?: boolean;
  }) => Promise<{ topic: string; partitions: FetchOffsetsPartition[] }[]>;
  fetchTopicOffsets: (topic: string) => Promise<TopicOffset[]>;
  fetchTopicOffsetsByTimestamp: (topic: string, timestamp?: OffsetInput) => Promise<SeekEntry[]>;
  setOffsets: (options: { groupId: string; topic: string; partitions: SeekInput[] }) => Promise<void>;
  resetOffsets: (options: { groupId: string; topic: string; earliest?: boolean }) => Promise<void>;
  describeConfigs: (options: {
    resources: ResourceConfigQuery[];
    includeSynonyms?: boolean;
    includeDocumentation?: boolean;
  }) => Promise<{ resources: DescribeConfigsResponseV2Body['resources'] }>;
  alterConfigs: (options: {
    resources: ResourceConfig[];
    validateOnly?: boolean;
  }) => Promise<{ resources: AlterConfigsResponseV1Body['resources'] }>;
  incrementalAlterConfigs: (options: {
    resources: IncrementalResourceConfig[];
    validateOnly?: boolean;
  }) => Promise<{ resources: IncrementalAlterConfigsResponseV1Body['resources'] }>;
  listGroups: () => Promise<{ groups: ListGroupsResponseV2Body['groups'] }>;
  describeGroups: (groupIds: string[]) => Promise<{ groups: DescribeGroupsResponseV2Body['groups'] }>;
  deleteGroups: (groupIds: string[]) => Promise<DeleteGroupsResult[]>;
  deleteGroupOffsets: (options: {
    groupId: string;
    topics: TopicPartitions[];
  }) => Promise<{ topics: OffsetDeleteResponseV0Body['topics'] }>;
  createAcls: (options: { acl: AclEntry[] }) => Promise<boolean>;
  describeAcls: (options: AclFilter) => Promise<{ resources: DescribeAclsResponseV1Body['resources'] }>;
  deleteAcls: (options: {
    filters: AclFilter[];
  }) => Promise<{ filterResponses: DeleteAclsResponseV1Body['filterResponses'] }>;
  alterPartitionReassignments: (options: { topics: PartitionReassignment[]; timeout?: number }) => Promise<void>;
  listPartitionReassignments: (options?: {
    topics?: TopicPartitions[] | null;
    timeout?: number;
  }) => Promise<{ topics: ListPartitionReassignmentsResponseV0Body['topics'] }>;
  electLeaders: (options: {
    topicPartitions?: TopicPartitions[] | null;
    electionType?: number;
    timeout?: number;
  }) => Promise<{ results: ElectLeadersResponseV0Body['results'] }>;
  describeUserScramCredentials: (options?: {
    users?: string[] | null;
  }) => Promise<{ results: DescribeUserScramCredentialsResponseV0Body['results'] }>;
  alterUserScramCredentials: (options: {
    deletions?: { name: string; mechanism: number }[];
    upsertions?: (
      | { name: string; mechanism: number; iterations?: number; password: string; salt?: Buffer }
      | { name: string; mechanism: number; iterations: number; salt: Buffer; saltedPassword: Buffer }
    )[];
  }) => Promise<{ results: AlterUserScramCredentialsResponseV0Body['results'] }>;
  describeClientQuotas: (options?: {
    components?: { entityType: string; matchType: number; match: string | null }[];
    strict?: boolean;
  }) => Promise<{ entries: DescribeClientQuotasResponseV1Body['entries'] }>;
  alterClientQuotas: (options: {
    entries: {
      entity: { entityType: string; entityName: string | null }[];
      ops: { key: string; value: number; remove: boolean }[];
    }[];
    validateOnly?: boolean;
  }) => Promise<{ entries: AlterClientQuotasResponseV1Body['entries'] }>;
  describeLogDirs: (options?: {
    topics?: { topic: string; partitions: number[] }[] | null;
    brokerIds?: Array<string | number>;
  }) => Promise<{ brokers: { brokerId: number; logDirs: DescribeLogDirsResponseV2Body['logDirs'] }[] }>;
  alterReplicaLogDirs: (options: {
    dirs: { path: string; topics: { topic: string; partitions: number[] }[] }[];
    brokerId: string | number;
  }) => Promise<{ results: AlterReplicaLogDirsResponseV2Body['results'] }>;
  on: (
    eventName: AdminEventName,
    listener: (event: InstrumentationEvent<unknown>) => void | Promise<void>,
  ) => RemoveInstrumentationEventListener;
  logger: () => Logger;
  readonly events: typeof events;
  [Symbol.asyncDispose]: () => Promise<void>;
}
