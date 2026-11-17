import { API_KEYS } from './api-keys';

/**
 * What a request family states about its Kafka API before any of its codecs load: the api key,
 * the protocol's name for it, and the first flexible version (KIP-482), from which the API uses
 * compact types, tagged fields, request header v2 and response header v1. `null` means the API
 * never becomes flexible.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export interface KafkaApi {
  readonly apiKey: number;
  readonly apiName: string;
  readonly firstFlexibleVersion: number | null;
}

function kafkaApi(apiName: keyof typeof API_KEYS, firstFlexibleVersion: number | null): KafkaApi {
  return Object.freeze({ apiKey: API_KEYS[apiName], apiName, firstFlexibleVersion });
}

/**
 * Every Kafka API this client implements. This module imports no codecs, so request framing can
 * read flexible versions from it without loading the families that `Broker#send` loads on demand.
 */
export const KAFKA_APIS = Object.freeze({
  Produce: kafkaApi('Produce', 9),
  Fetch: kafkaApi('Fetch', 12),
  ListOffsets: kafkaApi('ListOffsets', 6),
  Metadata: kafkaApi('Metadata', 9),
  OffsetCommit: kafkaApi('OffsetCommit', 8),
  OffsetFetch: kafkaApi('OffsetFetch', 6),
  GroupCoordinator: kafkaApi('GroupCoordinator', 3),
  JoinGroup: kafkaApi('JoinGroup', 6),
  Heartbeat: kafkaApi('Heartbeat', 4),
  LeaveGroup: kafkaApi('LeaveGroup', 4),
  SyncGroup: kafkaApi('SyncGroup', 4),
  DescribeGroups: kafkaApi('DescribeGroups', 5),
  ListGroups: kafkaApi('ListGroups', 3),
  SaslHandshake: kafkaApi('SaslHandshake', null),
  ApiVersions: kafkaApi('ApiVersions', 3),
  CreateTopics: kafkaApi('CreateTopics', 5),
  DeleteTopics: kafkaApi('DeleteTopics', 4),
  DeleteRecords: kafkaApi('DeleteRecords', 2),
  InitProducerId: kafkaApi('InitProducerId', 3),
  OffsetForLeaderEpoch: kafkaApi('OffsetForLeaderEpoch', 4),
  AddPartitionsToTxn: kafkaApi('AddPartitionsToTxn', 3),
  AddOffsetsToTxn: kafkaApi('AddOffsetsToTxn', 3),
  EndTxn: kafkaApi('EndTxn', 3),
  WriteTxnMarkers: kafkaApi('WriteTxnMarkers', 1),
  TxnOffsetCommit: kafkaApi('TxnOffsetCommit', 3),
  DescribeAcls: kafkaApi('DescribeAcls', 2),
  CreateAcls: kafkaApi('CreateAcls', 2),
  DeleteAcls: kafkaApi('DeleteAcls', 2),
  DescribeConfigs: kafkaApi('DescribeConfigs', 4),
  AlterConfigs: kafkaApi('AlterConfigs', 2),
  AlterReplicaLogDirs: kafkaApi('AlterReplicaLogDirs', 2),
  DescribeLogDirs: kafkaApi('DescribeLogDirs', 2),
  SaslAuthenticate: kafkaApi('SaslAuthenticate', 2),
  CreatePartitions: kafkaApi('CreatePartitions', 2),
  CreateDelegationToken: kafkaApi('CreateDelegationToken', 2),
  RenewDelegationToken: kafkaApi('RenewDelegationToken', 2),
  ExpireDelegationToken: kafkaApi('ExpireDelegationToken', 2),
  DescribeDelegationToken: kafkaApi('DescribeDelegationToken', 2),
  DeleteGroups: kafkaApi('DeleteGroups', 2),
  ElectLeaders: kafkaApi('ElectLeaders', 2),
  IncrementalAlterConfigs: kafkaApi('IncrementalAlterConfigs', 1),
  AlterPartitionReassignments: kafkaApi('AlterPartitionReassignments', 0),
  ListPartitionReassignments: kafkaApi('ListPartitionReassignments', 0),
  OffsetDelete: kafkaApi('OffsetDelete', 1),
  DescribeClientQuotas: kafkaApi('DescribeClientQuotas', 1),
  AlterClientQuotas: kafkaApi('AlterClientQuotas', 1),
  DescribeUserScramCredentials: kafkaApi('DescribeUserScramCredentials', 0),
  AlterUserScramCredentials: kafkaApi('AlterUserScramCredentials', 0),
  DescribeQuorum: kafkaApi('DescribeQuorum', 0),
  UpdateFeatures: kafkaApi('UpdateFeatures', 0),
  DescribeCluster: kafkaApi('DescribeCluster', 0),
  DescribeProducers: kafkaApi('DescribeProducers', 0),
  UnregisterBroker: kafkaApi('UnregisterBroker', 0),
  DescribeTransactions: kafkaApi('DescribeTransactions', 0),
  ListTransactions: kafkaApi('ListTransactions', 0),
  ConsumerGroupHeartbeat: kafkaApi('ConsumerGroupHeartbeat', 0),
  ConsumerGroupDescribe: kafkaApi('ConsumerGroupDescribe', 0),
  GetTelemetrySubscriptions: kafkaApi('GetTelemetrySubscriptions', 0),
  PushTelemetry: kafkaApi('PushTelemetry', 0),
  AssignReplicasToDirs: kafkaApi('AssignReplicasToDirs', 0),
  ListConfigResources: kafkaApi('ListConfigResources', 0),
  DescribeTopicPartitions: kafkaApi('DescribeTopicPartitions', 0),
  ShareGroupHeartbeat: kafkaApi('ShareGroupHeartbeat', 0),
  ShareGroupDescribe: kafkaApi('ShareGroupDescribe', 0),
  ShareFetch: kafkaApi('ShareFetch', 0),
  ShareAcknowledge: kafkaApi('ShareAcknowledge', 0),
  AddRaftVoter: kafkaApi('AddRaftVoter', 0),
  RemoveRaftVoter: kafkaApi('RemoveRaftVoter', 0),
  DescribeShareGroupOffsets: kafkaApi('DescribeShareGroupOffsets', 0),
  AlterShareGroupOffsets: kafkaApi('AlterShareGroupOffsets', 0),
  DeleteShareGroupOffsets: kafkaApi('DeleteShareGroupOffsets', 0),
});

/** First flexible version by api key; `undefined` for an API this client does not implement. */
const FIRST_FLEXIBLE_VERSIONS: (number | null | undefined)[] = [];
for (const api of Object.values(KAFKA_APIS)) {
  FIRST_FLEXIBLE_VERSIONS[api.apiKey] = api.firstFlexibleVersion;
}

function isFlexibleVersion(apiKey: number, apiVersion: number): boolean {
  const first = FIRST_FLEXIBLE_VERSIONS[apiKey];
  return first != null && apiVersion >= first;
}

/**
 * Whether the request is framed with header v2 (nullable `clientId` plus a trailing `TAG_BUFFER`).
 *
 * Includes ApiVersions v3+: Kafka's generated `requestHeaderVersion` returns 2 for flexible
 * versions. The broker reads `apiKey` and `apiVersion` first, then parses the remainder of the
 * header, so there is no chicken-and-egg problem on the request path.
 */
export function usesFlexibleRequestHeader(apiKey: number, apiVersion: number): boolean {
  return isFlexibleVersion(apiKey, apiVersion);
}

/**
 * Whether the response is framed with header v1 (`correlationId` plus a trailing `TAG_BUFFER`).
 *
 * ApiVersions is the exception: the response always uses header v0 (correlation id only) so
 * `error_code` stays at a fixed offset when the client does not yet know which versions the
 * broker speaks (KIP-511). Kafka's `ApiMessageTypeGenerator` hardcodes this.
 */
export function usesFlexibleResponseHeader(apiKey: number, apiVersion: number): boolean {
  if (apiKey === API_KEYS.ApiVersions) return false;
  return isFlexibleVersion(apiKey, apiVersion);
}
