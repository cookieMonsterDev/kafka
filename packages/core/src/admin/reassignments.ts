import { KafkaNonRetriableError } from '../errors';
import type { ElectLeadersResponseV0Body } from '../protocol/requests/elect-leaders/v0/response';
import type { ListPartitionReassignmentsResponseV0Body } from '../protocol/requests/list-partition-reassignments/v0/response';
import { CONTROLLER } from '../cluster/run';
import { retrier as createRetrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown } from './helpers';
import type { AlterPartitionReassignmentsOptions, TopicPartitions } from './types';
import {
  AlterPartitionReassignments,
  ElectLeaders,
  ListPartitionReassignments,
} from '../protocol/requests/lazy-families';

export interface ReassignmentsApi {
  alterPartitionReassignments: (options: AlterPartitionReassignmentsOptions) => Promise<void>;
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

export function createReassignmentsApi({ cluster, retry }: AdminContext): ReassignmentsApi {
  const retrier = createRetrier(retry);

  const alterPartitionReassignments = async ({
    topics,
    timeout,
    allowReplicationFactorChange,
  }: AlterPartitionReassignmentsOptions): Promise<void> => {
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

    await cluster.run(
      CONTROLLER,
      async (broker) => {
        await broker.send(AlterPartitionReassignments, { topics, timeout, allowReplicationFactorChange });
      },
      { retrier, refreshMetadata: true, description: 'Could not reassign partitions' },
    );
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

    return cluster.run(
      CONTROLLER,
      async (broker) => {
        const response = await broker.send(ListPartitionReassignments, { topics, timeout });
        return { topics: response.topics };
      },
      { retrier, refreshMetadata: true, description: 'Could not reassign partitions' },
    );
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

    return cluster.run(
      CONTROLLER,
      async (broker) => {
        const response = await broker.send(ElectLeaders, { topicPartitions, electionType, timeout });
        return { results: response.results };
      },
      { retrier, refreshMetadata: true, description: 'Could not elect leaders' },
    );
  };

  return { alterPartitionReassignments, listPartitionReassignments, electLeaders };
}
