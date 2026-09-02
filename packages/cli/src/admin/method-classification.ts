import type { Admin } from '@cookiemonsterdev/kafka-core';

export type MethodClassification = 'mounted' | 'passthrough-only';

/**
 * Every real admin operation on `Admin`, excluding lifecycle/introspection members (`on`,
 * `logger`, `events`, `[Symbol.asyncDispose]`) that are not something a user calls by name.
 * `Record<AdminMethodName, MethodClassification>` below is a mapped-type sentinel: adding a
 * method to core's `Admin` interface without adding it here is a `tsc` error, not a missed test —
 * the same pattern core itself uses for its `KafkaConfig` key exhaustiveness guard.
 */
export type AdminMethodName = Exclude<keyof Admin, 'on' | 'logger' | 'events' | typeof Symbol.asyncDispose>;

/**
 * `mounted` — reachable through a first-class command (or used internally by one, like
 * `fetchTopicMetadata`'s fallback in `topic describe`). `passthrough-only` — reachable only
 * through `admin call`/`admin methods` for now; a later command family promotes it to `mounted`.
 * Nothing is `out-of-scope`: every method here is reachable one way or the other.
 */
export const ADMIN_METHOD_CLASSIFICATION: Record<AdminMethodName, MethodClassification> = {
  connect: 'mounted',
  disconnect: 'mounted',
  clientInstanceId: 'passthrough-only',
  listTopics: 'mounted',
  createTopics: 'mounted',
  deleteTopics: 'mounted',
  createPartitions: 'mounted',
  fetchTopicMetadata: 'mounted',
  describeCluster: 'mounted',
  describeTopicPartitions: 'mounted',
  describeProducers: 'mounted',
  deleteTopicRecords: 'mounted',
  fetchOffsets: 'mounted',
  fetchTopicOffsets: 'mounted',
  fetchTopicOffsetsByTimestamp: 'mounted',
  setOffsets: 'passthrough-only',
  resetOffsets: 'mounted',
  describeConfigs: 'mounted',
  alterConfigs: 'passthrough-only',
  incrementalAlterConfigs: 'mounted',
  listGroups: 'mounted',
  describeGroups: 'mounted',
  describeClassicGroups: 'passthrough-only',
  describeConsumerGroups: 'passthrough-only',
  deleteGroups: 'mounted',
  deleteGroupOffsets: 'mounted',
  removeMembersFromConsumerGroup: 'mounted',
  describeShareGroups: 'passthrough-only',
  listShareGroupOffsets: 'passthrough-only',
  alterShareGroupOffsets: 'passthrough-only',
  deleteShareGroupOffsets: 'passthrough-only',
  deleteShareGroups: 'passthrough-only',
  createAcls: 'mounted',
  describeAcls: 'mounted',
  deleteAcls: 'mounted',
  alterPartitionReassignments: 'passthrough-only',
  listPartitionReassignments: 'passthrough-only',
  electLeaders: 'passthrough-only',
  describeUserScramCredentials: 'passthrough-only',
  alterUserScramCredentials: 'passthrough-only',
  describeClientQuotas: 'passthrough-only',
  alterClientQuotas: 'passthrough-only',
  describeLogDirs: 'mounted',
  describeReplicaLogDirs: 'passthrough-only',
  alterReplicaLogDirs: 'passthrough-only',
  updateFeatures: 'passthrough-only',
  describeFeatures: 'mounted',
  describeMetadataQuorum: 'mounted',
  unregisterBroker: 'passthrough-only',
  assignReplicasToDirs: 'passthrough-only',
  addRaftVoter: 'passthrough-only',
  removeRaftVoter: 'passthrough-only',
  listConfigResources: 'mounted',
  describeTransactions: 'passthrough-only',
  listTransactions: 'passthrough-only',
  fenceProducers: 'passthrough-only',
  abortTransaction: 'passthrough-only',
  forceTerminateTransaction: 'passthrough-only',
  createDelegationToken: 'passthrough-only',
  renewDelegationToken: 'passthrough-only',
  expireDelegationToken: 'passthrough-only',
  describeDelegationToken: 'passthrough-only',
};

/**
 * Methods that only read state — never require `--yes --force` in `admin call`. Kept as an
 * explicit allow-list (not "everything not obviously a write") so a new destructive method
 * defaults to requiring confirmation until someone deliberately allow-lists it.
 */
export const READ_ONLY_ADMIN_METHODS: ReadonlySet<AdminMethodName> = new Set<AdminMethodName>([
  'connect',
  'clientInstanceId',
  'listTopics',
  'fetchTopicMetadata',
  'describeCluster',
  'describeTopicPartitions',
  'describeProducers',
  'fetchOffsets',
  'fetchTopicOffsets',
  'fetchTopicOffsetsByTimestamp',
  'describeConfigs',
  'listGroups',
  'describeGroups',
  'describeClassicGroups',
  'describeConsumerGroups',
  'describeShareGroups',
  'listShareGroupOffsets',
  'describeAcls',
  'listPartitionReassignments',
  'describeUserScramCredentials',
  'describeClientQuotas',
  'describeLogDirs',
  'describeReplicaLogDirs',
  'describeFeatures',
  'describeMetadataQuorum',
  'listConfigResources',
  'describeTransactions',
  'listTransactions',
  'describeDelegationToken',
]);
