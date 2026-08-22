import { retrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { protocolType } from './helpers';
import type { DescribeMetadataQuorumResult } from './types';

export interface MetadataQuorumApi {
  describeMetadataQuorum: () => Promise<DescribeMetadataQuorumResult>;
}

export function createMetadataQuorumApi({ cluster, logger, retry }: AdminContext): MetadataQuorumApi {
  const describeMetadataQuorum = async (): Promise<DescribeMetadataQuorumResult> => {
    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        const { topics } = await broker.describeQuorum({
          topics: [{ topicName: '__cluster_metadata', partitions: [{ partitionIndex: 0 }] }],
        });
        return {
          topics: topics.map(({ topicName, partitions }) => ({
            topicName,
            partitions: partitions.map(
              ({ partitionIndex, errorCode, leaderId, leaderEpoch, highWatermark, currentVoters, observers }) => ({
                partitionIndex,
                errorCode,
                leaderId,
                leaderEpoch,
                highWatermark,
                currentVoters: currentVoters.map(({ replicaId, logEndOffset }) => ({ replicaId, logEndOffset })),
                observers: observers.map(({ replicaId, logEndOffset }) => ({ replicaId, logEndOffset })),
              }),
            ),
          })),
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
