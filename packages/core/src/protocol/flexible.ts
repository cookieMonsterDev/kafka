import { API_KEYS } from './requests/api-keys';

/**
 * First version at which each API uses compact types, tagged fields, request header v2, and
 * response header v1 (KIP-482). APIs omitted from this map never become flexible.
 *
 * ApiVersions is flexible from v3. The broker peeks at `apiKey`/`apiVersion` before parsing the
 * rest of the header, so v3+ requests use header v2. The *response* header stays v0 — see
 * `usesFlexibleResponseHeader` and KIP-511.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const FIRST_FLEXIBLE_VERSION: Readonly<Record<number, number>> = Object.freeze({
  [API_KEYS.Produce]: 9,
  [API_KEYS.Fetch]: 12,
  [API_KEYS.ListOffsets]: 6,
  [API_KEYS.Metadata]: 9,
  [API_KEYS.OffsetCommit]: 8,
  [API_KEYS.OffsetFetch]: 6,
  [API_KEYS.GroupCoordinator]: 3,
  [API_KEYS.JoinGroup]: 6,
  [API_KEYS.Heartbeat]: 4,
  [API_KEYS.LeaveGroup]: 4,
  [API_KEYS.SyncGroup]: 4,
  [API_KEYS.DescribeGroups]: 5,
  [API_KEYS.ListGroups]: 3,
  [API_KEYS.ApiVersions]: 3,
  [API_KEYS.CreateTopics]: 5,
  [API_KEYS.DeleteTopics]: 4,
  [API_KEYS.DeleteRecords]: 2,
  [API_KEYS.InitProducerId]: 3,
  [API_KEYS.OffsetForLeaderEpoch]: 3,
  [API_KEYS.AddPartitionsToTxn]: 3,
  [API_KEYS.AddOffsetsToTxn]: 3,
  [API_KEYS.EndTxn]: 3,
  [API_KEYS.TxnOffsetCommit]: 3,
  [API_KEYS.DescribeAcls]: 2,
  [API_KEYS.CreateAcls]: 2,
  [API_KEYS.DeleteAcls]: 2,
  [API_KEYS.DescribeConfigs]: 4,
  [API_KEYS.AlterConfigs]: 2,
  [API_KEYS.AlterReplicaLogDirs]: 2,
  [API_KEYS.DescribeLogDirs]: 2,
  [API_KEYS.SaslAuthenticate]: 2,
  [API_KEYS.CreatePartitions]: 2,
  [API_KEYS.CreateDelegationToken]: 2,
  [API_KEYS.RenewDelegationToken]: 2,
  [API_KEYS.ExpireDelegationToken]: 2,
  [API_KEYS.DescribeDelegationToken]: 2,
  [API_KEYS.DeleteGroups]: 2,
  [API_KEYS.ElectLeaders]: 2,
  [API_KEYS.IncrementalAlterConfigs]: 1,
  [API_KEYS.AlterPartitionReassignments]: 0,
  [API_KEYS.ListPartitionReassignments]: 0,
  [API_KEYS.OffsetDelete]: 1,
  [API_KEYS.DescribeClientQuotas]: 1,
  [API_KEYS.AlterClientQuotas]: 1,
  [API_KEYS.DescribeUserScramCredentials]: 0,
  [API_KEYS.AlterUserScramCredentials]: 0,
  [API_KEYS.UpdateFeatures]: 0,
  [API_KEYS.DescribeCluster]: 0,
  [API_KEYS.DescribeProducers]: 0,
  [API_KEYS.DescribeTransactions]: 0,
});

/**
 * Version at which `apiKey` becomes a flexible (KIP-482) protocol. `undefined` means the API
 * never uses compact types or tagged-field headers.
 */
export function firstFlexibleVersion(apiKey: number): number | undefined {
  return FIRST_FLEXIBLE_VERSION[apiKey];
}

export function isFlexibleVersion(apiKey: number, apiVersion: number): boolean {
  const first = firstFlexibleVersion(apiKey);
  return first !== undefined && apiVersion >= first;
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
