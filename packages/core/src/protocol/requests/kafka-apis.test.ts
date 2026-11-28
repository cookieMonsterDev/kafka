import { describe, expect, it } from 'vitest';
import { API_KEYS } from './api-keys';
import { KAFKA_APIS, usesFlexibleRequestHeader, usesFlexibleResponseHeader } from './kafka-apis';

describe('protocol/requests/kafka-apis', () => {
  it('names each API by its api key', () => {
    for (const [name, api] of Object.entries(KAFKA_APIS)) {
      expect(api).toEqual({
        apiKey: API_KEYS[name as keyof typeof API_KEYS],
        apiName: name,
        firstFlexibleVersion: api.firstFlexibleVersion,
      });
    }
  });

  it('states the first flexible version of each API, or null when it never becomes flexible', () => {
    expect(KAFKA_APIS.Produce.firstFlexibleVersion).toBe(9);
    expect(KAFKA_APIS.Fetch.firstFlexibleVersion).toBe(12);
    expect(KAFKA_APIS.ListOffsets.firstFlexibleVersion).toBe(6);
    expect(KAFKA_APIS.Metadata.firstFlexibleVersion).toBe(9);
    expect(KAFKA_APIS.OffsetCommit.firstFlexibleVersion).toBe(8);
    expect(KAFKA_APIS.OffsetFetch.firstFlexibleVersion).toBe(6);
    expect(KAFKA_APIS.GroupCoordinator.firstFlexibleVersion).toBe(3);
    expect(KAFKA_APIS.JoinGroup.firstFlexibleVersion).toBe(6);
    expect(KAFKA_APIS.Heartbeat.firstFlexibleVersion).toBe(4);
    expect(KAFKA_APIS.LeaveGroup.firstFlexibleVersion).toBe(4);
    expect(KAFKA_APIS.SyncGroup.firstFlexibleVersion).toBe(4);
    expect(KAFKA_APIS.DescribeGroups.firstFlexibleVersion).toBe(5);
    expect(KAFKA_APIS.ListGroups.firstFlexibleVersion).toBe(3);
    expect(KAFKA_APIS.ApiVersions.firstFlexibleVersion).toBe(3);
    expect(KAFKA_APIS.CreateTopics.firstFlexibleVersion).toBe(5);
    expect(KAFKA_APIS.DeleteTopics.firstFlexibleVersion).toBe(4);
    expect(KAFKA_APIS.DeleteRecords.firstFlexibleVersion).toBe(2);
    expect(KAFKA_APIS.InitProducerId.firstFlexibleVersion).toBe(3);
    expect(KAFKA_APIS.OffsetForLeaderEpoch.firstFlexibleVersion).toBe(4);
    expect(KAFKA_APIS.AddPartitionsToTxn.firstFlexibleVersion).toBe(3);
    expect(KAFKA_APIS.AddOffsetsToTxn.firstFlexibleVersion).toBe(3);
    expect(KAFKA_APIS.EndTxn.firstFlexibleVersion).toBe(3);
    expect(KAFKA_APIS.WriteTxnMarkers.firstFlexibleVersion).toBe(1);
    expect(KAFKA_APIS.TxnOffsetCommit.firstFlexibleVersion).toBe(3);
    expect(KAFKA_APIS.DescribeAcls.firstFlexibleVersion).toBe(2);
    expect(KAFKA_APIS.CreateAcls.firstFlexibleVersion).toBe(2);
    expect(KAFKA_APIS.DeleteAcls.firstFlexibleVersion).toBe(2);
    expect(KAFKA_APIS.DescribeConfigs.firstFlexibleVersion).toBe(4);
    expect(KAFKA_APIS.AlterConfigs.firstFlexibleVersion).toBe(2);
    expect(KAFKA_APIS.AlterReplicaLogDirs.firstFlexibleVersion).toBe(2);
    expect(KAFKA_APIS.DescribeLogDirs.firstFlexibleVersion).toBe(2);
    expect(KAFKA_APIS.SaslAuthenticate.firstFlexibleVersion).toBe(2);
    expect(KAFKA_APIS.CreatePartitions.firstFlexibleVersion).toBe(2);
    expect(KAFKA_APIS.CreateDelegationToken.firstFlexibleVersion).toBe(2);
    expect(KAFKA_APIS.RenewDelegationToken.firstFlexibleVersion).toBe(2);
    expect(KAFKA_APIS.ExpireDelegationToken.firstFlexibleVersion).toBe(2);
    expect(KAFKA_APIS.DescribeDelegationToken.firstFlexibleVersion).toBe(2);
    expect(KAFKA_APIS.DeleteGroups.firstFlexibleVersion).toBe(2);
    expect(KAFKA_APIS.ElectLeaders.firstFlexibleVersion).toBe(2);
    expect(KAFKA_APIS.IncrementalAlterConfigs.firstFlexibleVersion).toBe(1);
    expect(KAFKA_APIS.AlterPartitionReassignments.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.ListPartitionReassignments.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.OffsetDelete.firstFlexibleVersion).toBe(1);
    expect(KAFKA_APIS.DescribeUserScramCredentials.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.AlterUserScramCredentials.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.AssignReplicasToDirs.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.DescribeQuorum.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.UpdateFeatures.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.DescribeCluster.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.UnregisterBroker.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.DescribeProducers.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.DescribeTransactions.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.ListTransactions.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.ConsumerGroupHeartbeat.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.ConsumerGroupDescribe.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.GetTelemetrySubscriptions.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.PushTelemetry.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.ShareGroupHeartbeat.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.ShareGroupDescribe.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.ShareFetch.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.ShareAcknowledge.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.DescribeShareGroupOffsets.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.AlterShareGroupOffsets.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.DeleteShareGroupOffsets.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.ListConfigResources.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.DescribeTopicPartitions.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.AddRaftVoter.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.RemoveRaftVoter.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.UnregisterController.firstFlexibleVersion).toBe(0);
    expect(KAFKA_APIS.SaslHandshake.firstFlexibleVersion).toBeNull();
  });

  it('treats versions at or above the first flexible version as flexible', () => {
    expect(usesFlexibleRequestHeader(999, 0)).toBe(false);
    expect(usesFlexibleRequestHeader(API_KEYS.Produce, 8)).toBe(false);
    expect(usesFlexibleRequestHeader(API_KEYS.Produce, 9)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.Metadata, 6)).toBe(false);
    expect(usesFlexibleRequestHeader(API_KEYS.AlterPartitionReassignments, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.SaslHandshake, 1)).toBe(false);
  });

  it('uses request header v2 for flexible APIs including ApiVersions v3+', () => {
    expect(usesFlexibleRequestHeader(API_KEYS.Metadata, 6)).toBe(false);
    expect(usesFlexibleRequestHeader(API_KEYS.Metadata, 9)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.AlterPartitionReassignments, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.WriteTxnMarkers, 1)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.WriteTxnMarkers, 2)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.DescribeQuorum, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.UpdateFeatures, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.UpdateFeatures, 2)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.UnregisterBroker, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.UnregisterController, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.AssignReplicasToDirs, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.AddRaftVoter, 1)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.RemoveRaftVoter, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.DescribeProducers, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.DescribeTransactions, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.ListTransactions, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.ConsumerGroupHeartbeat, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.ConsumerGroupDescribe, 1)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.GetTelemetrySubscriptions, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.PushTelemetry, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.ShareGroupHeartbeat, 1)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.ShareFetch, 1)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.ShareAcknowledge, 1)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.DescribeShareGroupOffsets, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.AlterShareGroupOffsets, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.DeleteShareGroupOffsets, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.ListConfigResources, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.ListConfigResources, 1)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.DescribeTopicPartitions, 0)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.CreateDelegationToken, 1)).toBe(false);
    expect(usesFlexibleRequestHeader(API_KEYS.CreateDelegationToken, 2)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.RenewDelegationToken, 2)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.ExpireDelegationToken, 2)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.DescribeDelegationToken, 3)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.ApiVersions, 0)).toBe(false);
    expect(usesFlexibleRequestHeader(API_KEYS.ApiVersions, 3)).toBe(true);
    expect(usesFlexibleRequestHeader(API_KEYS.ApiVersions, 4)).toBe(true);
  });

  it('uses response header v1 for flexible APIs except ApiVersions', () => {
    expect(usesFlexibleResponseHeader(API_KEYS.Metadata, 6)).toBe(false);
    expect(usesFlexibleResponseHeader(API_KEYS.Metadata, 9)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.AlterPartitionReassignments, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.WriteTxnMarkers, 1)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.WriteTxnMarkers, 2)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.DescribeQuorum, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.UpdateFeatures, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.UpdateFeatures, 2)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.UnregisterBroker, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.UnregisterController, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.AssignReplicasToDirs, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.AddRaftVoter, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.RemoveRaftVoter, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.DescribeProducers, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.DescribeTransactions, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.ListTransactions, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.ConsumerGroupHeartbeat, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.ConsumerGroupDescribe, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.ShareGroupHeartbeat, 1)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.ShareFetch, 1)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.ShareAcknowledge, 1)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.DescribeShareGroupOffsets, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.AlterShareGroupOffsets, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.DeleteShareGroupOffsets, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.ListConfigResources, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.ListConfigResources, 1)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.DescribeTopicPartitions, 0)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.CreateDelegationToken, 1)).toBe(false);
    expect(usesFlexibleResponseHeader(API_KEYS.CreateDelegationToken, 2)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.DescribeDelegationToken, 3)).toBe(true);
    expect(usesFlexibleResponseHeader(API_KEYS.ApiVersions, 2)).toBe(false);
    expect(usesFlexibleResponseHeader(API_KEYS.ApiVersions, 3)).toBe(false);
    expect(usesFlexibleResponseHeader(API_KEYS.ApiVersions, 4)).toBe(false);
  });
});
