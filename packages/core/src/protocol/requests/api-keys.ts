/**
 * Numeric Kafka API keys used in request headers.
 * `GroupCoordinator` is FindCoordinator (10); `ElectLeaders` (43) was originally
 * named ElectPreferredLeaders in the protocol.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const API_KEYS = Object.freeze({
  Produce: 0,
  Fetch: 1,
  ListOffsets: 2,
  Metadata: 3,
  LeaderAndIsr: 4,
  StopReplica: 5,
  UpdateMetadata: 6,
  ControlledShutdown: 7,
  OffsetCommit: 8,
  OffsetFetch: 9,
  GroupCoordinator: 10,
  JoinGroup: 11,
  Heartbeat: 12,
  LeaveGroup: 13,
  SyncGroup: 14,
  DescribeGroups: 15,
  ListGroups: 16,
  SaslHandshake: 17,
  ApiVersions: 18,
  CreateTopics: 19,
  DeleteTopics: 20,
  DeleteRecords: 21,
  InitProducerId: 22,
  OffsetForLeaderEpoch: 23,
  AddPartitionsToTxn: 24,
  AddOffsetsToTxn: 25,
  EndTxn: 26,
  WriteTxnMarkers: 27,
  TxnOffsetCommit: 28,
  DescribeAcls: 29,
  CreateAcls: 30,
  DeleteAcls: 31,
  DescribeConfigs: 32,
  AlterConfigs: 33,
  AlterReplicaLogDirs: 34,
  DescribeLogDirs: 35,
  SaslAuthenticate: 36,
  CreatePartitions: 37,
  CreateDelegationToken: 38,
  RenewDelegationToken: 39,
  ExpireDelegationToken: 40,
  DescribeDelegationToken: 41,
  DeleteGroups: 42,
  ElectLeaders: 43, // originally ElectPreferredLeaders
  IncrementalAlterConfigs: 44,
  AlterPartitionReassignments: 45,
  ListPartitionReassignments: 46,
  OffsetDelete: 47,
  DescribeClientQuotas: 48,
  AlterClientQuotas: 49,
  DescribeUserScramCredentials: 50,
  AlterUserScramCredentials: 51,
  UpdateFeatures: 57,
  DescribeCluster: 60,
  DescribeProducers: 61,
  DescribeTransactions: 65,
  ListTransactions: 66,
  ConsumerGroupHeartbeat: 68,
  ConsumerGroupDescribe: 69,
});

export type ApiKey = (typeof API_KEYS)[keyof typeof API_KEYS];

const NAMES_BY_KEY: ReadonlyMap<number, string> = new Map(Object.entries(API_KEYS).map(([name, key]) => [key, name]));

export function apiKeyName(apiKey: number): string | undefined {
  return NAMES_BY_KEY.get(apiKey);
}
