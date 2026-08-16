import { KafkaNonRetriableError } from '../errors';
import type { AlterReplicaLogDir } from '../protocol/requests/alter-replica-log-dirs/v0/request';
import type { AlterReplicaLogDirsResponseV2Body } from '../protocol/requests/alter-replica-log-dirs/v2/response';
import type { DescribeLogDirsTopic } from '../protocol/requests/describe-log-dirs/v0/request';
import type { DescribeLogDirsResponseV2Body } from '../protocol/requests/describe-log-dirs/v2/response';
import { retrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown, protocolType } from './helpers';

export interface LogDirsApi {
  describeLogDirs: (options?: {
    topics?: DescribeLogDirsTopic[] | null;
    brokerIds?: Array<string | number>;
  }) => Promise<{ brokers: { brokerId: number; logDirs: DescribeLogDirsResponseV2Body['logDirs'] }[] }>;
  alterReplicaLogDirs: (options: {
    dirs: AlterReplicaLogDir[];
    brokerId: string | number;
  }) => Promise<{ results: AlterReplicaLogDirsResponseV2Body['results'] }>;
}

export function createLogDirsApi({ cluster, logger, retry }: AdminContext): LogDirsApi {
  const describeLogDirs = async (
    options: { topics?: DescribeLogDirsTopic[] | null; brokerIds?: Array<string | number> } = {},
  ): Promise<{ brokers: { brokerId: number; logDirs: DescribeLogDirsResponseV2Body['logDirs'] }[] }> => {
    return retrier(retry)(async (bail) => {
      try {
        await cluster.refreshMetadata();
        const nodeIds = options.brokerIds?.map(String) ?? Object.keys(cluster.brokerPool.brokers);
        if (nodeIds.length === 0) {
          throw new KafkaNonRetriableError('No brokers available to describe log dirs');
        }

        const brokers = await Promise.all(
          nodeIds.map(async (nodeId) => {
            const broker = await cluster.findBroker({ nodeId });
            const { logDirs } = await broker.describeLogDirs({ topics: options.topics });
            return { brokerId: Number(nodeId), logDirs };
          }),
        );
        return { brokers };
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER' || protocolType(error) === 'NOT_LEADER_OR_FOLLOWER') {
          await cluster.refreshMetadata();
          throw error;
        }
        bail(error as Error);
        throw error;
      }
    });
  };

  const alterReplicaLogDirs = async ({
    dirs,
    brokerId,
  }: {
    dirs: AlterReplicaLogDir[];
    brokerId: string | number;
  }): Promise<{ results: AlterReplicaLogDirsResponseV2Body['results'] }> => {
    if (!Array.isArray(dirs) || dirs.length === 0) {
      throw new KafkaNonRetriableError(`Invalid replica log dir list ${formatUnknown(dirs)}`);
    }

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findBroker({ nodeId: String(brokerId) });
        const { results } = await broker.alterReplicaLogDirs({ dirs });
        return { results };
      } catch (error) {
        logger.warn('Could not alter replica log dirs', {
          error: error instanceof Error ? error.message : String(error),
          brokerId,
          retryCount,
          retryTime,
        });
        if (protocolType(error) === 'NOT_LEADER_OR_FOLLOWER') {
          await cluster.refreshMetadata();
          throw error;
        }
        bail(error as Error);
        throw error;
      }
    });
  };

  return { describeLogDirs, alterReplicaLogDirs };
}
