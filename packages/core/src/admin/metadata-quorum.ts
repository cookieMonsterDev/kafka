import { CONTROLLER } from '../cluster/run';
import { retrier as createRetrier } from '../retry/index';
import type { AdminContext } from './helpers';
import type { DescribeMetadataQuorumResult, MetadataQuorumReplica } from './types';
import { DescribeQuorum } from '../protocol/requests/lazy-families';

export interface MetadataQuorumApi {
  describeMetadataQuorum: () => Promise<DescribeMetadataQuorumResult>;
}

function mapReplica(replica: {
  replicaId: number;
  logEndOffset: bigint;
  lastFetchTimestamp?: bigint;
  lastCaughtUpTimestamp?: bigint;
  replicaDirectoryId?: Buffer;
}): MetadataQuorumReplica {
  return {
    replicaId: replica.replicaId,
    logEndOffset: replica.logEndOffset,
    ...(replica.lastFetchTimestamp != null ? { lastFetchTimestamp: replica.lastFetchTimestamp } : {}),
    ...(replica.lastCaughtUpTimestamp != null ? { lastCaughtUpTimestamp: replica.lastCaughtUpTimestamp } : {}),
    ...(replica.replicaDirectoryId != null ? { replicaDirectoryId: replica.replicaDirectoryId } : {}),
  };
}

export interface MetadataQuorumApi {
  describeMetadataQuorum: () => Promise<DescribeMetadataQuorumResult>;
}

export function createMetadataQuorumApi({ cluster, retry }: AdminContext): MetadataQuorumApi {
  const retrier = createRetrier(retry);

  const describeMetadataQuorum = async (): Promise<DescribeMetadataQuorumResult> => {
    return cluster.run(
      CONTROLLER,
      async (broker) => {
        const response = await broker.send(DescribeQuorum, {
          topics: [{ topicName: '__cluster_metadata', partitions: [{ partitionIndex: 0 }] }],
        });
        return {
          ...('errorMessage' in response ? { errorMessage: response.errorMessage } : {}),
          topics: response.topics.map(({ topicName, partitions }) => ({
            topicName,
            partitions: partitions.map((partition) => ({
              partitionIndex: partition.partitionIndex,
              errorCode: partition.errorCode,
              ...('errorMessage' in partition ? { errorMessage: partition.errorMessage } : {}),
              leaderId: partition.leaderId,
              leaderEpoch: partition.leaderEpoch,
              highWatermark: partition.highWatermark,
              currentVoters: partition.currentVoters.map(mapReplica),
              observers: partition.observers.map(mapReplica),
            })),
          })),
          ...('nodes' in response ? { nodes: response.nodes } : {}),
        };
      },
      { retrier, refreshMetadata: true, description: 'Could not describe metadata quorum' },
    );
  };

  return { describeMetadataQuorum };
}
