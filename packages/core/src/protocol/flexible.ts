import { API_KEYS } from './requests/api-keys';

/**
 * First version at which each API uses compact types, tagged fields, request header v2, and
 * response header v1 (KIP-482). APIs omitted from this map never become flexible.
 *
 * ApiVersions is flexible from v3, but its *request* header stays v1 — see
 * `usesFlexibleRequestHeader`.
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
  [API_KEYS.SaslAuthenticate]: 2,
  [API_KEYS.CreatePartitions]: 2,
  [API_KEYS.DeleteGroups]: 2,
  [API_KEYS.ElectPreferredLeaders]: 2,
  [API_KEYS.IncrementalAlterConfigs]: 1,
  [API_KEYS.AlterPartitionReassignments]: 0,
  [API_KEYS.ListPartitionReassignments]: 0,
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
 * ApiVersions never uses a flexible request header, even at v3+, because the broker must parse
 * the header before it knows which ApiVersions body version the client speaks (KIP-511 / KIP-482).
 * The ApiVersions v3 *body* is still flexible.
 */
export function usesFlexibleRequestHeader(apiKey: number, apiVersion: number): boolean {
  if (apiKey === API_KEYS.ApiVersions) return false;
  return isFlexibleVersion(apiKey, apiVersion);
}

/**
 * Whether the response is framed with header v1 (`correlationId` plus a trailing `TAG_BUFFER`).
 * ApiVersions v3+ does use a flexible response header.
 */
export function usesFlexibleResponseHeader(apiKey: number, apiVersion: number): boolean {
  return isFlexibleVersion(apiKey, apiVersion);
}
