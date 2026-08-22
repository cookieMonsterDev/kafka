import { retrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { protocolType } from './helpers';
import type { DescribeMetadataQuorumResult, MetadataQuorumReplica } from './types';

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

export function createMetadataQuorumApi({ cluster, logger, retry }: AdminContext): MetadataQuorumApi {
  const describeMetadataQuorum = async (): Promise<DescribeMetadataQuorumResult> => {
    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        const response = await broker.describeQuorum({
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
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not describe metadata quorum', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          await cluster.refreshMetadata();
          throw error;
        }
        bail(error as Error);
        throw error;
      }
    });
  };

  return { describeMetadataQuorum };
}
