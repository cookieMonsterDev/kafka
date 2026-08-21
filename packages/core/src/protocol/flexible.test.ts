import { describe, expect, it } from 'vitest';
import {
  firstFlexibleVersion,
  isFlexibleVersion,
  usesFlexibleRequestHeader,
  usesFlexibleResponseHeader,
} from './flexible';
import { API_KEYS } from './requests/api-keys';

describe('protocol/flexible', () => {
  it('returns the first flexible version for known APIs and undefined otherwise', () => {
    expect(firstFlexibleVersion(API_KEYS.Produce)).toBe(9);
    expect(firstFlexibleVersion(API_KEYS.Fetch)).toBe(12);
    expect(firstFlexibleVersion(API_KEYS.ListOffsets)).toBe(6);
    expect(firstFlexibleVersion(API_KEYS.Metadata)).toBe(9);
    expect(firstFlexibleVersion(API_KEYS.OffsetCommit)).toBe(8);
    expect(firstFlexibleVersion(API_KEYS.OffsetFetch)).toBe(6);
    expect(firstFlexibleVersion(API_KEYS.GroupCoordinator)).toBe(3);
    expect(firstFlexibleVersion(API_KEYS.JoinGroup)).toBe(6);
    expect(firstFlexibleVersion(API_KEYS.Heartbeat)).toBe(4);
    expect(firstFlexibleVersion(API_KEYS.LeaveGroup)).toBe(4);
    expect(firstFlexibleVersion(API_KEYS.SyncGroup)).toBe(4);
    expect(firstFlexibleVersion(API_KEYS.DescribeGroups)).toBe(5);
    expect(firstFlexibleVersion(API_KEYS.ListGroups)).toBe(3);
    expect(firstFlexibleVersion(API_KEYS.ApiVersions)).toBe(3);
    expect(firstFlexibleVersion(API_KEYS.CreateTopics)).toBe(5);
    expect(firstFlexibleVersion(API_KEYS.DeleteTopics)).toBe(4);
    expect(firstFlexibleVersion(API_KEYS.DeleteRecords)).toBe(2);
    expect(firstFlexibleVersion(API_KEYS.InitProducerId)).toBe(3);
    expect(firstFlexibleVersion(API_KEYS.OffsetForLeaderEpoch)).toBe(3);
    expect(firstFlexibleVersion(API_KEYS.AddPartitionsToTxn)).toBe(3);
    expect(firstFlexibleVersion(API_KEYS.AddOffsetsToTxn)).toBe(3);
    expect(firstFlexibleVersion(API_KEYS.EndTxn)).toBe(3);
    expect(firstFlexibleVersion(API_KEYS.TxnOffsetCommit)).toBe(3);
    expect(firstFlexibleVersion(API_KEYS.DescribeAcls)).toBe(2);
    expect(firstFlexibleVersion(API_KEYS.CreateAcls)).toBe(2);
    expect(firstFlexibleVersion(API_KEYS.DeleteAcls)).toBe(2);
    expect(firstFlexibleVersion(API_KEYS.DescribeConfigs)).toBe(4);
    expect(firstFlexibleVersion(API_KEYS.AlterConfigs)).toBe(2);
    expect(firstFlexibleVersion(API_KEYS.AlterReplicaLogDirs)).toBe(2);
    expect(firstFlexibleVersion(API_KEYS.DescribeLogDirs)).toBe(2);
    expect(firstFlexibleVersion(API_KEYS.SaslAuthenticate)).toBe(2);
    expect(firstFlexibleVersion(API_KEYS.CreatePartitions)).toBe(2);
    expect(firstFlexibleVersion(API_KEYS.DeleteGroups)).toBe(2);
    expect(firstFlexibleVersion(API_KEYS.ElectLeaders)).toBe(2);
    expect(firstFlexibleVersion(API_KEYS.IncrementalAlterConfigs)).toBe(1);
    expect(firstFlexibleVersion(API_KEYS.AlterPartitionReassignments)).toBe(0);
    expect(firstFlexibleVersion(API_KEYS.ListPartitionReassignments)).toBe(0);
    expect(firstFlexibleVersion(API_KEYS.OffsetDelete)).toBe(1);
    expect(firstFlexibleVersion(API_KEYS.DescribeUserScramCredentials)).toBe(0);
    expect(firstFlexibleVersion(API_KEYS.AlterUserScramCredentials)).toBe(0);
    expect(firstFlexibleVersion(API_KEYS.UpdateFeatures)).toBe(0);
    expect(firstFlexibleVersion(API_KEYS.DescribeCluster)).toBe(0);
    expect(firstFlexibleVersion(API_KEYS.DescribeProducers)).toBe(0);
    expect(firstFlexibleVersion(API_KEYS.DescribeTransactions)).toBe(0);
    expect(firstFlexibleVersion(API_KEYS.DescribeTopicPartitions)).toBe(0);
    expect(firstFlexibleVersion(API_KEYS.SaslHandshake)).toBeUndefined();
    expect(firstFlexibleVersion(999)).toBeUndefined();
  });

  it('treats versions at or above the first flexible version as flexible', () => {
    expect(isFlexibleVersion(API_KEYS.Produce, 8)).toBe(false);
    expect(isFlexibleVersion(API_KEYS.Produce, 9)).toBe(true);
    expect(isFlexibleVersion(API_KEYS.Metadata, 6)).toBe(false);
    expect(isFlexibleVersion(API_KEYS.AlterPartitionReassignments, 0)).toBe(true);
    expect(isFlexibleVersion(API_KEYS.SaslHandshake, 1)).toBe(false);
  });

  it('uses request header v2 for flexible APIs including ApiVersions v3+', () => {
    expect(usesFlexibleRequestHeader(API_KEYS.Metadata, 6)).toBe(false);
    expect(usesFlexibleRequestHeader(API_KEYS.Metadata, 9)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.AlterPartitionReassignments, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.UpdateFeatures, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.UpdateFeatures, 2)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.DescribeProducers, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.DescribeTransactions, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.DescribeTopicPartitions, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.ApiVersions, 0)).toBe(false);
    expect(usesFlexibleRequestHeader(API_KEYS.ApiVersions, 3)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.ApiVersions, 4)).toBe(true);
  });

  it('uses response header v1 for flexible APIs except ApiVersions', () => {
    expect(usesFlexibleResponseHeader(API_KEYS.Metadata, 6)).toBe(false);
    expect(usesFlexibleResponseHeader(API_KEYS.Metadata, 9)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.AlterPartitionReassignments, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.UpdateFeatures, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.UpdateFeatures, 2)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.DescribeProducers, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.DescribeTransactions, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.DescribeTopicPartitions, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.ApiVersions, 2)).toBe(false);
    expect(usesFlexibleResponseHeader(API_KEYS.ApiVersions, 3)).toBe(false);
    expect(usesFlexibleResponseHeader(API_KEYS.ApiVersions, 4)).toBe(false);
  });
});
