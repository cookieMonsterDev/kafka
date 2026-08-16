import { KafkaNonRetriableError } from '../errors';
import type { ElectLeadersResponseV0Body } from '../protocol/requests/elect-leaders/v0/response';
import type { ListPartitionReassignmentsResponseV0Body } from '../protocol/requests/list-partition-reassignments/v0/response';
import { retrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { protocolType, formatUnknown } from './helpers';
import type { PartitionReassignment, TopicPartitions } from './types';

export interface ReassignmentsApi {
  alterPartitionReassignments: (options: { topics: PartitionReassignment[]; timeout?: number }) => Promise<void>;
  listPartitionReassignments: (options?: {
    topics?: TopicPartitions[] | null;
    timeout?: number;
  }) => Promise<{ topics: ListPartitionReassignmentsResponseV0Body['topics'] }>;
  electLeaders: (options: {
    topicPartitions?: TopicPartitions[] | null;
    electionType?: number;
    timeout?: number;
  }) => Promise<{ results: ElectLeadersResponseV0Body['results'] }>;
}

export function createReassignmentsApi({ cluster, logger, retry }: AdminContext): ReassignmentsApi {
  const alterPartitionReassignments = async ({
    topics,
    timeout,
  }: {
    topics: PartitionReassignment[];
    timeout?: number;
  }): Promise<void> => {
    if (!topics || !Array.isArray(topics)) {
      throw new KafkaNonRetriableError(`Invalid topics array ${formatUnknown(topics)}`);
    }

    if (topics.filter(({ topic }) => typeof topic !== 'string').length > 0) {
      throw new KafkaNonRetriableError('Invalid topics array, the topic names have to be a valid string');
    }

    const topicNames = new Set(topics.map(({ topic }) => topic));
    if (topicNames.size < topics.length) {
      throw new KafkaNonRetriableError('Invalid topics array, it cannot have multiple entries for the same topic');
    }

    for (const { topic, partitionAssignment } of topics) {
      if (!partitionAssignment || !Array.isArray(partitionAssignment)) {
        throw new KafkaNonRetriableError(
          `Invalid partitions array: ${formatUnknown(partitionAssignment)} for topic: ${topic}`,
        );
      }

      for (const { partition, replicas } of partitionAssignment) {
        if (partition === null || partition === undefined || typeof partition !== 'number' || partition < 0) {
          throw new KafkaNonRetriableError(`Invalid partitions index: ${partition} for topic: ${topic}`);
        }

        if (!replicas || !Array.isArray(replicas)) {
          throw new KafkaNonRetriableError(
            `Invalid replica assignment: ${formatUnknown(replicas)} for topic: ${topic} on partition: ${partition}`,
          );
        }

        if (replicas.filter((replica) => typeof replica !== 'number' || replica < 0).length >= 1) {
          throw new KafkaNonRetriableError(
            `Invalid replica assignment: ${formatUnknown(replicas)} for topic: ${topic} on partition: ${partition}. Replicas must be a non negative number`,
          );
        }
      }
    }

    await retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        await broker.alterPartitionReassignments({ topics, timeout });
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not reassign partitions', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          throw error;
        }

        bail(error as Error);
      }
    });
  };

  const listPartitionReassignments = async ({
    topics = null,
    timeout,
  }: {
    topics?: TopicPartitions[] | null;
    timeout?: number;
  } = {}): Promise<{ topics: ListPartitionReassignmentsResponseV0Body['topics'] }> => {
    if (topics) {
      if (!Array.isArray(topics)) {
        throw new KafkaNonRetriableError(`Invalid topics array ${formatUnknown(topics)}`);
      }

      if (topics.filter(({ topic }) => typeof topic !== 'string').length > 0) {
        throw new KafkaNonRetriableError('Invalid topics array, the topic names have to be a valid string');
      }

      const topicNames = new Set(topics.map(({ topic }) => topic));
      if (topicNames.size < topics.length) {
        throw new KafkaNonRetriableError('Invalid topics array, it cannot have multiple entries for the same topic');
      }

      for (const { topic, partitions } of topics) {
        if (!partitions || !Array.isArray(partitions)) {
          throw new KafkaNonRetriableError(`Invalid partition array: ${formatUnknown(partitions)} for topic: ${topic}`);
        }

        if (partitions.filter((partition) => typeof partition !== 'number' || partition < 0).length >= 1) {
          throw new KafkaNonRetriableError(
            `Invalid partition array: ${formatUnknown(partitions)} for topic: ${topic}. The partition indices have to be a valid number greater than 0.`,
          );
        }
      }
    }

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        const response = await broker.listPartitionReassignments({ topics, timeout });
        return { topics: response.topics };
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not reassign partitions', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          throw error;
        }

        bail(error as Error);
        return { topics: [] };
      }
    });
  };

  const electLeaders = async ({
    topicPartitions = null,
    electionType,
    timeout,
  }: {
    topicPartitions?: TopicPartitions[] | null;
    electionType?: number;
    timeout?: number;
  } = {}): Promise<{ results: ElectLeadersResponseV0Body['results'] }> => {
    if (topicPartitions) {
      if (!Array.isArray(topicPartitions)) {
        throw new KafkaNonRetriableError(`Invalid topicPartitions array ${formatUnknown(topicPartitions)}`);
      }

      if (topicPartitions.filter(({ topic }) => typeof topic !== 'string').length > 0) {
        throw new KafkaNonRetriableError('Invalid topicPartitions array, the topic names have to be a valid string');
      }

      const topicNames = new Set(topicPartitions.map(({ topic }) => topic));
      if (topicNames.size < topicPartitions.length) {
        throw new KafkaNonRetriableError(
          'Invalid topicPartitions array, it cannot have multiple entries for the same topic',
        );
      }

      for (const { topic, partitions } of topicPartitions) {
        if (!partitions || !Array.isArray(partitions)) {
          throw new KafkaNonRetriableError(`Invalid partition array: ${formatUnknown(partitions)} for topic: ${topic}`);
        }

        if (partitions.filter((partition) => typeof partition !== 'number' || partition < 0).length >= 1) {
          throw new KafkaNonRetriableError(
            `Invalid partition array: ${formatUnknown(partitions)} for topic: ${topic}. The partition indices have to be a valid number greater than 0.`,
          );
        }
      }
    }

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        const response = await broker.electLeaders({ topicPartitions, electionType, timeout });
        return { results: response.results };
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not elect leaders', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          throw error;
        }

        bail(error as Error);
        return { results: [] };
      }
    });
  };

  return { alterPartitionReassignments, listPartitionReassignments, electLeaders };
}
