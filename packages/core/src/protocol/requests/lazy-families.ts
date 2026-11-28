import { lazyRequestFamily } from './index';
import { KAFKA_APIS } from './kafka-apis';

/*
 * Stubs for the families in `./lazy-requests`: each states its API up front and imports the
 * `lazy-requests` chunk on its first `Broker#send`. Import families from here, never from their own
 * modules, wherever the code is reachable from `src/index.ts`; a static import of a family module
 * would put its codecs back in the startup graph.
 */
export const AddRaftVoter = lazyRequestFamily(
  KAFKA_APIS.AddRaftVoter,
  async () => (await import('./lazy-requests')).AddRaftVoter,
);
export const AlterClientQuotas = lazyRequestFamily(
  KAFKA_APIS.AlterClientQuotas,
  async () => (await import('./lazy-requests')).AlterClientQuotas,
);
export const AlterConfigs = lazyRequestFamily(
  KAFKA_APIS.AlterConfigs,
  async () => (await import('./lazy-requests')).AlterConfigs,
);
export const AlterPartitionReassignments = lazyRequestFamily(
  KAFKA_APIS.AlterPartitionReassignments,
  async () => (await import('./lazy-requests')).AlterPartitionReassignments,
);
export const AlterReplicaLogDirs = lazyRequestFamily(
  KAFKA_APIS.AlterReplicaLogDirs,
  async () => (await import('./lazy-requests')).AlterReplicaLogDirs,
);
export const AlterShareGroupOffsets = lazyRequestFamily(
  KAFKA_APIS.AlterShareGroupOffsets,
  async () => (await import('./lazy-requests')).AlterShareGroupOffsets,
);
export const AlterUserScramCredentials = lazyRequestFamily(
  KAFKA_APIS.AlterUserScramCredentials,
  async () => (await import('./lazy-requests')).AlterUserScramCredentials,
);
export const AssignReplicasToDirs = lazyRequestFamily(
  KAFKA_APIS.AssignReplicasToDirs,
  async () => (await import('./lazy-requests')).AssignReplicasToDirs,
);
export const ConsumerGroupDescribe = lazyRequestFamily(
  KAFKA_APIS.ConsumerGroupDescribe,
  async () => (await import('./lazy-requests')).ConsumerGroupDescribe,
);
export const CreateAcls = lazyRequestFamily(
  KAFKA_APIS.CreateAcls,
  async () => (await import('./lazy-requests')).CreateAcls,
);
export const CreateDelegationToken = lazyRequestFamily(
  KAFKA_APIS.CreateDelegationToken,
  async () => (await import('./lazy-requests')).CreateDelegationToken,
);
export const CreatePartitions = lazyRequestFamily(
  KAFKA_APIS.CreatePartitions,
  async () => (await import('./lazy-requests')).CreatePartitions,
);
export const CreateTopics = lazyRequestFamily(
  KAFKA_APIS.CreateTopics,
  async () => (await import('./lazy-requests')).CreateTopics,
);
export const DeleteAcls = lazyRequestFamily(
  KAFKA_APIS.DeleteAcls,
  async () => (await import('./lazy-requests')).DeleteAcls,
);
export const DeleteGroups = lazyRequestFamily(
  KAFKA_APIS.DeleteGroups,
  async () => (await import('./lazy-requests')).DeleteGroups,
);
export const DeleteRecords = lazyRequestFamily(
  KAFKA_APIS.DeleteRecords,
  async () => (await import('./lazy-requests')).DeleteRecords,
);
export const DeleteShareGroupOffsets = lazyRequestFamily(
  KAFKA_APIS.DeleteShareGroupOffsets,
  async () => (await import('./lazy-requests')).DeleteShareGroupOffsets,
);
export const DeleteTopics = lazyRequestFamily(
  KAFKA_APIS.DeleteTopics,
  async () => (await import('./lazy-requests')).DeleteTopics,
);
export const DescribeAcls = lazyRequestFamily(
  KAFKA_APIS.DescribeAcls,
  async () => (await import('./lazy-requests')).DescribeAcls,
);
export const DescribeClientQuotas = lazyRequestFamily(
  KAFKA_APIS.DescribeClientQuotas,
  async () => (await import('./lazy-requests')).DescribeClientQuotas,
);
export const DescribeCluster = lazyRequestFamily(
  KAFKA_APIS.DescribeCluster,
  async () => (await import('./lazy-requests')).DescribeCluster,
);
export const DescribeConfigs = lazyRequestFamily(
  KAFKA_APIS.DescribeConfigs,
  async () => (await import('./lazy-requests')).DescribeConfigs,
);
export const DescribeDelegationToken = lazyRequestFamily(
  KAFKA_APIS.DescribeDelegationToken,
  async () => (await import('./lazy-requests')).DescribeDelegationToken,
);
export const DescribeLogDirs = lazyRequestFamily(
  KAFKA_APIS.DescribeLogDirs,
  async () => (await import('./lazy-requests')).DescribeLogDirs,
);
export const DescribeProducers = lazyRequestFamily(
  KAFKA_APIS.DescribeProducers,
  async () => (await import('./lazy-requests')).DescribeProducers,
);
export const DescribeQuorum = lazyRequestFamily(
  KAFKA_APIS.DescribeQuorum,
  async () => (await import('./lazy-requests')).DescribeQuorum,
);
export const DescribeShareGroupOffsets = lazyRequestFamily(
  KAFKA_APIS.DescribeShareGroupOffsets,
  async () => (await import('./lazy-requests')).DescribeShareGroupOffsets,
);
export const DescribeTopicPartitions = lazyRequestFamily(
  KAFKA_APIS.DescribeTopicPartitions,
  async () => (await import('./lazy-requests')).DescribeTopicPartitions,
);
export const DescribeTransactions = lazyRequestFamily(
  KAFKA_APIS.DescribeTransactions,
  async () => (await import('./lazy-requests')).DescribeTransactions,
);
export const DescribeUserScramCredentials = lazyRequestFamily(
  KAFKA_APIS.DescribeUserScramCredentials,
  async () => (await import('./lazy-requests')).DescribeUserScramCredentials,
);
export const ElectLeaders = lazyRequestFamily(
  KAFKA_APIS.ElectLeaders,
  async () => (await import('./lazy-requests')).ElectLeaders,
);
export const ExpireDelegationToken = lazyRequestFamily(
  KAFKA_APIS.ExpireDelegationToken,
  async () => (await import('./lazy-requests')).ExpireDelegationToken,
);
export const IncrementalAlterConfigs = lazyRequestFamily(
  KAFKA_APIS.IncrementalAlterConfigs,
  async () => (await import('./lazy-requests')).IncrementalAlterConfigs,
);
export const ListConfigResources = lazyRequestFamily(
  KAFKA_APIS.ListConfigResources,
  async () => (await import('./lazy-requests')).ListConfigResources,
);
export const ListGroups = lazyRequestFamily(
  KAFKA_APIS.ListGroups,
  async () => (await import('./lazy-requests')).ListGroups,
);
export const ListPartitionReassignments = lazyRequestFamily(
  KAFKA_APIS.ListPartitionReassignments,
  async () => (await import('./lazy-requests')).ListPartitionReassignments,
);
export const ListTransactions = lazyRequestFamily(
  KAFKA_APIS.ListTransactions,
  async () => (await import('./lazy-requests')).ListTransactions,
);
export const OffsetDelete = lazyRequestFamily(
  KAFKA_APIS.OffsetDelete,
  async () => (await import('./lazy-requests')).OffsetDelete,
);
export const RemoveRaftVoter = lazyRequestFamily(
  KAFKA_APIS.RemoveRaftVoter,
  async () => (await import('./lazy-requests')).RemoveRaftVoter,
);
export const RenewDelegationToken = lazyRequestFamily(
  KAFKA_APIS.RenewDelegationToken,
  async () => (await import('./lazy-requests')).RenewDelegationToken,
);
export const ShareGroupDescribe = lazyRequestFamily(
  KAFKA_APIS.ShareGroupDescribe,
  async () => (await import('./lazy-requests')).ShareGroupDescribe,
);
export const UnregisterBroker = lazyRequestFamily(
  KAFKA_APIS.UnregisterBroker,
  async () => (await import('./lazy-requests')).UnregisterBroker,
);
export const UnregisterController = lazyRequestFamily(
  KAFKA_APIS.UnregisterController,
  async () => (await import('./lazy-requests')).UnregisterController,
);
export const UpdateFeatures = lazyRequestFamily(
  KAFKA_APIS.UpdateFeatures,
  async () => (await import('./lazy-requests')).UpdateFeatures,
);
export const WriteTxnMarkers = lazyRequestFamily(
  KAFKA_APIS.WriteTxnMarkers,
  async () => (await import('./lazy-requests')).WriteTxnMarkers,
);
