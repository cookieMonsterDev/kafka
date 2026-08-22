import { KafkaNonRetriableError, KafkaProtocolError } from '../errors';
import type { AlterReplicaLogDir } from '../protocol/requests/alter-replica-log-dirs/v0/request';
import type { AlterReplicaLogDirsResponseV2Body } from '../protocol/requests/alter-replica-log-dirs/v2/response';
import type { DescribeLogDirsTopic } from '../protocol/requests/describe-log-dirs/v0/request';
import type { DescribeLogDirsResponseV2Body } from '../protocol/requests/describe-log-dirs/v2/response';
import { retrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown, protocolType } from './helpers';
import type { DescribeReplicaLogDirsReplica, DescribeReplicaLogDirsResult } from './types';

export interface LogDirsApi {
  describeLogDirs: (options?: {
    topics?: DescribeLogDirsTopic[] | null;
    brokerIds?: Array<string | number>;
  }) => Promise<{ brokers: { brokerId: number; logDirs: DescribeLogDirsResponseV2Body['logDirs'] }[] }>;
  alterReplicaLogDirs: (options: {
    dirs: AlterReplicaLogDir[];
    brokerId: string | number;
  }) => Promise<{ results: AlterReplicaLogDirsResponseV2Body['results'] }>;
  describeReplicaLogDirs: (replicas: DescribeReplicaLogDirsReplica[]) => Promise<{
    replicas: DescribeReplicaLogDirsResult[];
  }>;
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

  const describeReplicaLogDirs = async (
    replicas: DescribeReplicaLogDirsReplica[],
  ): Promise<{ replicas: DescribeReplicaLogDirsResult[] }> => {
    if (!Array.isArray(replicas) || replicas.length === 0) {
      throw new KafkaNonRetriableError(`Invalid replica list ${formatUnknown(replicas)}`);
    }

    for (const replica of replicas) {
      if (typeof replica.topic !== 'string' || replica.topic.length === 0) {
        throw new KafkaNonRetriableError(`Invalid topic ${formatUnknown(replica.topic)}`);
      }
      if (!Number.isInteger(replica.partition) || replica.partition < 0) {
        throw new KafkaNonRetriableError(
          `Invalid partition ${formatUnknown(replica.partition)} for topic ${replica.topic}`,
        );
      }
      if (replica.brokerId == null || (typeof replica.brokerId !== 'number' && typeof replica.brokerId !== 'string')) {
        throw new KafkaNonRetriableError(`Invalid brokerId ${formatUnknown(replica.brokerId)}`);
      }
    }

    return retrier(retry)(async (bail) => {
      try {
        await cluster.refreshMetadata();

        const replicasByBroker = new Map<string, DescribeReplicaLogDirsReplica[]>();
        for (const replica of replicas) {
          const nodeId = String(replica.brokerId);
          const existing = replicasByBroker.get(nodeId) ?? [];
          existing.push(replica);
          replicasByBroker.set(nodeId, existing);
        }

        const brokerResults = await Promise.all(
          [...replicasByBroker.entries()].map(async ([nodeId, brokerReplicas]) => {
            const topicsByName = new Map<string, number[]>();
            for (const { topic, partition } of brokerReplicas) {
              const partitions = topicsByName.get(topic) ?? [];
              if (!partitions.includes(partition)) partitions.push(partition);
              topicsByName.set(topic, partitions);
            }
            const topics: DescribeLogDirsTopic[] = [...topicsByName.entries()].map(([topic, partitions]) => ({
              topic,
              partitions: partitions.sort((a, b) => a - b),
            }));

            try {
              const broker = await cluster.findBroker({ nodeId });
              const { logDirs } = await broker.describeLogDirs({ topics });
              return { nodeId, brokerReplicas, logDirs, errorCode: 0 };
            } catch (error) {
              if (error instanceof KafkaNonRetriableError) throw error;
              const code = error instanceof KafkaProtocolError ? (error.code ?? -1) : -1;
              return { nodeId, brokerReplicas, logDirs: [], errorCode: code };
            }
          }),
        );

        const results: DescribeReplicaLogDirsResult[] = [];
        for (const { nodeId, brokerReplicas, logDirs, errorCode } of brokerResults) {
          const brokerId = Number(nodeId);
          for (const replica of brokerReplicas) {
            if (errorCode !== 0) {
              results.push({
                topic: replica.topic,
                partition: replica.partition,
                brokerId,
                logDir: null,
                errorCode,
              });
              continue;
            }

            let match: DescribeReplicaLogDirsResult | null = null;
            for (const logDir of logDirs) {
              if (logDir.errorCode !== 0) continue;
              for (const topic of logDir.topics) {
                if (topic.topic !== replica.topic) continue;
                for (const partition of topic.partitions) {
                  if (partition.partition !== replica.partition) continue;
                  match = {
                    topic: replica.topic,
                    partition: replica.partition,
                    brokerId,
                    logDir: logDir.logDir,
                    errorCode: 0,
                    size: partition.size,
                    offsetLag: partition.offsetLag,
                    isFuture: partition.isFuture,
                  };
                }
              }
            }

            results.push(
              match ?? {
                topic: replica.topic,
                partition: replica.partition,
                brokerId,
                logDir: null,
                errorCode: 0,
              },
            );
          }
        }

        return { replicas: results };
      } catch (error) {
        if (protocolType(error) === 'NOT_LEADER_OR_FOLLOWER') {
          await cluster.refreshMetadata();
          throw error;
        }
        bail(error as Error);
        throw error;
      }
    });
  };

  return { describeLogDirs, alterReplicaLogDirs, describeReplicaLogDirs };
}
