import type { Cluster, PartitionMetadata } from '../cluster/index.js';
import type { InstrumentationEventEmitter, RemoveInstrumentationEventListener } from '../instrumentation/emitter.js';
import type { InstrumentationEvent } from '../instrumentation/event.js';
import type { Logger } from '../loggers/index.js';
import type { AclOperationType } from '../protocol/enums/acl-operation-types.js';
import type { AclPermissionType } from '../protocol/enums/acl-permission-types.js';
import type { AclResourceType } from '../protocol/enums/acl-resource-types.js';
import type { ConfigResourceType } from '../protocol/enums/config-resource-types.js';
import type { ResourcePatternType } from '../protocol/enums/resource-pattern-types.js';
import type { AlterConfigsResponseV1Body } from '../protocol/requests/alter-configs/v1/response.js';
import type { DescribeAclsResponseV1Body } from '../protocol/requests/describe-acls/v1/response.js';
import type { DescribeConfigsResponseV2Body } from '../protocol/requests/describe-configs/v2/response.js';
import type { DescribeGroupsResponseV2Body } from '../protocol/requests/describe-groups/v2/response.js';
import type { DeleteAclsResponseV1Body } from '../protocol/requests/delete-acls/v1/response.js';
import type { DeleteGroupsResult } from '../protocol/requests/delete-groups/v0/response.js';
import type { ListGroupsResponseV2Body } from '../protocol/requests/list-groups/v2/response.js';
import type { ListPartitionReassignmentsResponseV0Body } from '../protocol/requests/list-partition-reassignments/v0/response.js';
import type { RetryOptions } from '../retry/index.js';
import type { AdminEventName } from './instrumentation-events.js';
import { events } from './instrumentation-events.js';

export type OffsetInput = bigint | number | string;

export interface ReplicaAssignment {
  partition: number;
  replicas: number[];
}

export interface ResourceConfigEntry {
  name: string;
  value: string;
}

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

export interface Admin {
  connect: () => Promise<void>;
  disconnect: () => Promise<void>;
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
  }) => Promise<{ resources: DescribeConfigsResponseV2Body['resources'] }>;
  alterConfigs: (options: {
    resources: ResourceConfig[];
    validateOnly?: boolean;
  }) => Promise<{ resources: AlterConfigsResponseV1Body['resources'] }>;
  listGroups: () => Promise<{ groups: ListGroupsResponseV2Body['groups'] }>;
  describeGroups: (groupIds: string[]) => Promise<{ groups: DescribeGroupsResponseV2Body['groups'] }>;
  deleteGroups: (groupIds: string[]) => Promise<DeleteGroupsResult[]>;
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
  on: (
    eventName: AdminEventName,
    listener: (event: InstrumentationEvent<unknown>) => void | Promise<void>,
  ) => RemoveInstrumentationEventListener;
  logger: () => Logger;
  readonly events: typeof events;
}
