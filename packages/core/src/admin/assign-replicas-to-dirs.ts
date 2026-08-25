import { KafkaNonRetriableError } from '../errors';
import { retrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown, protocolType } from './helpers';
import type { AssignReplicasToDirsOptions, AssignReplicasToDirsReplica } from './types';

export interface AssignReplicasToDirsApi {
  assignReplicasToDirs: (options: AssignReplicasToDirsOptions) => Promise<void>;
}

function validateBrokerId(brokerId: unknown): number {
  if (!Number.isInteger(brokerId) || (brokerId as number) < 0) {
    throw new KafkaNonRetriableError(`Invalid brokerId ${formatUnknown(brokerId)}`);
  }
  return brokerId as number;
}

function validateBrokerEpoch(brokerEpoch: unknown): bigint {
  if (brokerEpoch === undefined) return -1n;
  if (typeof brokerEpoch !== 'bigint') {
    throw new KafkaNonRetriableError(`Invalid brokerEpoch ${formatUnknown(brokerEpoch)}`);
  }
  return brokerEpoch;
}

function validateDirectoryId(directoryId: unknown): Buffer {
  if (!(directoryId instanceof Buffer) || directoryId.length !== 16) {
    throw new KafkaNonRetriableError(`Invalid directoryId ${formatUnknown(directoryId)}`);
  }
  return directoryId;
}

function validateReplicas(replicas: unknown): AssignReplicasToDirsReplica[] {
  if (!Array.isArray(replicas) || replicas.length === 0) {
    throw new KafkaNonRetriableError(`Invalid replica list ${formatUnknown(replicas)}`);
  }

  return replicas.map((replica) => {
    if (typeof replica !== 'object' || replica == null) {
      throw new KafkaNonRetriableError(`Invalid replica ${formatUnknown(replica)}`);
    }
    const { topic, partition, directoryId } = replica as AssignReplicasToDirsReplica;
    if (typeof topic !== 'string' || topic.length === 0) {
      throw new KafkaNonRetriableError(`Invalid topic ${formatUnknown(topic)}`);
    }
    if (!Number.isInteger(partition) || partition < 0) {
      throw new KafkaNonRetriableError(`Invalid partition ${formatUnknown(partition)}`);
    }
    return { topic, partition, directoryId: validateDirectoryId(directoryId) };
  });
}

function groupReplicasByDirectory(
  replicas: readonly (AssignReplicasToDirsReplica & { topicId: Buffer })[],
): { id: Buffer; topics: { topicId: Buffer; partitions: { partitionIndex: number }[] }[] }[] {
  const directories: {
    id: Buffer;
    topics: Map<string, { topicId: Buffer; partitions: { partitionIndex: number }[] }>;
  }[] = [];
  const directoryIndex = new Map<string, number>();

  for (const replica of replicas) {
    const dirKey = replica.directoryId.toString('hex');
    let dirIdx = directoryIndex.get(dirKey);
    if (dirIdx == null) {
      dirIdx = directories.length;
      directoryIndex.set(dirKey, dirIdx);
      directories.push({ id: replica.directoryId, topics: new Map() });
    }

    const directory = directories[dirIdx];
    if (directory == null) {
      throw new KafkaNonRetriableError(`Invariant violated: missing directory group for ${dirKey}`);
    }

    const topicKey = replica.topicId.toString('hex');
    let topic = directory.topics.get(topicKey);
    if (topic == null) {
      topic = { topicId: replica.topicId, partitions: [] };
      directory.topics.set(topicKey, topic);
    }

    if (!topic.partitions.some((entry) => entry.partitionIndex === replica.partition)) {
      topic.partitions.push({ partitionIndex: replica.partition });
    }
  }

  return directories.map((directory) => ({
    id: directory.id,
    topics: [...directory.topics.values()],
  }));
}

export function createAssignReplicasToDirsApi({ cluster, logger, retry }: AdminContext): AssignReplicasToDirsApi {
  const assignReplicasToDirs = async ({
    brokerId,
    brokerEpoch,
    replicas,
  }: AssignReplicasToDirsOptions): Promise<void> => {
    const normalizedBrokerId = validateBrokerId(brokerId);
    const normalizedBrokerEpoch = validateBrokerEpoch(brokerEpoch);
    const normalizedReplicas = validateReplicas(replicas);
    const topics = [...new Set(normalizedReplicas.map((replica) => replica.topic))];

    await retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.addMultipleTargetTopics(topics);
        await cluster.refreshMetadata();
        const directories = groupReplicasByDirectory(
          normalizedReplicas.map((replica) => {
            const topicId = cluster.findTopicId(replica.topic);
            if (topicId == null) {
              throw new KafkaNonRetriableError(`No topic id for topic ${replica.topic}`);
            }
            return { ...replica, topicId };
          }),
        );
        const broker = await cluster.findControllerBroker();
        await broker.assignReplicasToDirs({
          brokerId: normalizedBrokerId,
          brokerEpoch: normalizedBrokerEpoch,
          directories,
        });
      } catch (error) {
        if (error instanceof KafkaNonRetriableError) {
          bail(error);
          throw error;
        }
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not assign replicas to directories', {
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

  return { assignReplicasToDirs };
}
