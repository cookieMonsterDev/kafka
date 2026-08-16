import {
  KafkaAggregateError,
  KafkaBrokerNotFound,
  KafkaDeleteTopicRecordsError,
  KafkaNonRetriableError,
} from '../errors';
import { staleMetadata } from '../protocol/error-codes';
import { API_KEYS } from '../protocol/requests/api-keys';
import { retrier } from '../retry/index';
import { parseOffset } from '../consumer/types';
import type { AdminContext } from './helpers';
import { protocolType, requireMetadata, retryOnLeaderNotAvailable, formatUnknown } from './helpers';
import type { SeekInput, TopicConfig, TopicMetadata, TopicOffset, TopicPartitionConfig } from './types';

const NO_CONTROLLER_ID = -1;

export interface TopicsApi {
  listTopics: () => Promise<string[]>;
  createTopics: (options: {
    topics: TopicConfig[];
    validateOnly?: boolean;
    timeout?: number;
    waitForLeaders?: boolean;
  }) => Promise<boolean>;
  deleteTopics: (options: { topics: string[]; timeout?: number }) => Promise<void>;
  createPartitions: (options: {
    topicPartitions: TopicPartitionConfig[];
    validateOnly?: boolean;
    timeout?: number;
  }) => Promise<void>;
  fetchTopicMetadata: (options?: { topics?: string[] }) => Promise<{ topics: TopicMetadata[] }>;
  describeCluster: () => Promise<{
    brokers: { nodeId: number; host: string; port: number }[];
    controller: number | null;
    clusterId: string | null;
  }>;
  deleteTopicRecords: (options: { topic: string; partitions: SeekInput[] }) => Promise<void>;
}

export function createTopicsApi(
  { cluster, logger, retry }: AdminContext,
  { fetchTopicOffsets }: { fetchTopicOffsets: (topic: string) => Promise<TopicOffset[]> },
): TopicsApi {
  const listTopics = async (): Promise<string[]> => {
    const { topicMetadata } = await requireMetadata(cluster);
    return topicMetadata.map((topic) => topic.topic);
  };

  const createTopics = async ({
    topics,
    validateOnly,
    timeout,
    waitForLeaders = true,
  }: {
    topics: TopicConfig[];
    validateOnly?: boolean;
    timeout?: number;
    waitForLeaders?: boolean;
  }): Promise<boolean> => {
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

    for (const { topic, configEntries } of topics) {
      if (configEntries == null) continue;

      if (!Array.isArray(configEntries)) {
        throw new KafkaNonRetriableError(`Invalid configEntries for topic "${topic}", must be an array`);
      }

      configEntries.forEach((entry, index) => {
        if (typeof entry !== 'object' || entry == null) {
          throw new KafkaNonRetriableError(
            `Invalid configEntries for topic "${topic}". Entry ${index} must be an object`,
          );
        }

        for (const requiredProperty of ['name', 'value'] as const) {
          if (
            !Object.prototype.hasOwnProperty.call(entry, requiredProperty) ||
            typeof entry[requiredProperty] !== 'string'
          ) {
            throw new KafkaNonRetriableError(
              `Invalid configEntries for topic "${topic}". Entry ${index} must have a valid "${requiredProperty}" property`,
            );
          }
        }
      });
    }

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        await broker.createTopics({ topics, validateOnly, timeout });

        if (waitForLeaders) {
          const topicNamesArray = [...topicNames];
          await retryOnLeaderNotAvailable(async () => broker.metadata(topicNamesArray), {
            delay: 100,
            maxWait: timeout,
            timeoutMessage: 'Timed out while waiting for topic leaders',
          });
        }

        return true;
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not create topics', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          throw error;
        }

        if (error instanceof KafkaAggregateError) {
          if (error.errors.every((entry) => protocolType(entry) === 'TOPIC_ALREADY_EXISTS')) {
            return false;
          }
        }

        bail(error as Error);
        return false;
      }
    });
  };

  const createPartitions = async ({
    topicPartitions,
    validateOnly,
    timeout,
  }: {
    topicPartitions: TopicPartitionConfig[];
    validateOnly?: boolean;
    timeout?: number;
  }): Promise<void> => {
    if (!topicPartitions || !Array.isArray(topicPartitions)) {
      throw new KafkaNonRetriableError(`Invalid topic partitions array ${formatUnknown(topicPartitions)}`);
    }
    if (topicPartitions.length === 0) {
      throw new KafkaNonRetriableError('Empty topic partitions array');
    }

    if (topicPartitions.filter(({ topic }) => typeof topic !== 'string').length > 0) {
      throw new KafkaNonRetriableError('Invalid topic partitions array, the topic names have to be a valid string');
    }

    const topicNames = new Set(topicPartitions.map(({ topic }) => topic));
    if (topicNames.size < topicPartitions.length) {
      throw new KafkaNonRetriableError(
        'Invalid topic partitions array, it cannot have multiple entries for the same topic',
      );
    }

    await retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        await broker.createPartitions({ topicPartitions, validateOnly, timeout });
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not create topics', {
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

  const deleteTopics = async ({ topics, timeout }: { topics: string[]; timeout?: number }): Promise<void> => {
    if (!topics || !Array.isArray(topics)) {
      throw new KafkaNonRetriableError(`Invalid topics array ${formatUnknown(topics)}`);
    }

    if (topics.filter((topic) => typeof topic !== 'string').length > 0) {
      throw new KafkaNonRetriableError('Invalid topics array, the names must be a valid string');
    }

    await retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        await broker.deleteTopics({ topics, timeout });

        for (const topic of topics) {
          cluster.targetTopics.delete(topic);
        }

        await cluster.refreshMetadata();
      } catch (error) {
        const type = protocolType(error);
        if (type === 'NOT_CONTROLLER' || type === 'UNKNOWN_TOPIC_OR_PARTITION') {
          logger.warn('Could not delete topics', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          throw error;
        }

        if (type === 'REQUEST_TIMED_OUT') {
          logger.error(
            'Could not delete topics, check if "delete.topic.enable" is set to "true" (the default value is "false") or increase the timeout',
            {
              error: error instanceof Error ? error.message : String(error),
              retryCount,
              retryTime,
            },
          );
        }

        bail(error as Error);
      }
    });
  };

  const fetchTopicMetadata = async ({ topics = [] }: { topics?: string[] } = {}): Promise<{
    topics: TopicMetadata[];
  }> => {
    if (topics) {
      for (const topic of topics) {
        if (!topic || typeof topic !== 'string') {
          throw new KafkaNonRetriableError(`Invalid topic ${topic}`);
        }
      }
    }

    const metadata = await requireMetadata(cluster, { topics });

    return {
      topics: metadata.topicMetadata.map((topicMetadata) => ({
        name: topicMetadata.topic,
        partitions: topicMetadata.partitionMetadata,
      })),
    };
  };

  const describeCluster = async () => {
    await cluster.refreshMetadata();
    const versions = cluster.brokerPool.versions;
    if (versions?.[API_KEYS.DescribeCluster]) {
      const broker = await cluster.findControllerBroker();
      const body = await broker.describeCluster({});
      const controller = body.controllerId == null || body.controllerId === NO_CONTROLLER_ID ? null : body.controllerId;
      return {
        brokers: body.brokers.map(({ nodeId, host, port }) => ({ nodeId, host, port })),
        controller,
        clusterId: body.clusterId,
      };
    }

    const { brokers: nodes, clusterId, controllerId } = await requireMetadata(cluster, { topics: [] });
    const brokers = nodes.map(({ nodeId, host, port }) => ({ nodeId, host, port }));
    const controller = controllerId == null || controllerId === NO_CONTROLLER_ID ? null : controllerId;

    return { brokers, controller, clusterId };
  };

  const deleteTopicRecords = async ({
    topic,
    partitions,
  }: {
    topic: string;
    partitions: SeekInput[];
  }): Promise<void> => {
    if (!topic || typeof topic !== 'string') {
      throw new KafkaNonRetriableError(`Invalid topic "${topic}"`);
    }

    if (!partitions || partitions.length === 0) {
      throw new KafkaNonRetriableError('Invalid partitions');
    }

    const parsedPartitions = partitions.map(({ partition, offset }) => ({
      partition,
      offset: parseOffset(offset),
    }));

    const topicOffsets = await fetchTopicOffsets(topic);
    const partitionsByBroker = cluster.findLeaderForPartitions(
      topic,
      parsedPartitions.map((entry) => entry.partition),
    );

    const partitionsFound = Object.values(partitionsByBroker).flat();
    const leaderNotFoundErrors: { partition: number; offset: bigint; error: KafkaBrokerNotFound }[] = [];
    for (const { partition, offset } of parsedPartitions) {
      if (!partitionsFound.includes(partition)) {
        leaderNotFoundErrors.push({
          partition,
          offset,
          error: new KafkaBrokerNotFound('Could not find the leader for the partition', { retriable: false }),
        });
        continue;
      }

      const bounds = topicOffsets.find((entry) => entry.partition === partition);
      if (bounds != null && offset < bounds.low && offset !== -1n) {
        logger.warn(
          'The requested offset is before the earliest offset maintained on the partition - no records will be deleted from this partition',
          { topic, partition, offset },
        );
      }
    }

    if (leaderNotFoundErrors.length > 0) {
      throw new KafkaDeleteTopicRecordsError({ partitions: leaderNotFoundErrors });
    }

    const seekEntriesByBroker = new Map<
      string,
      { topic: string; partitions: { partition: number; offset: bigint }[] }
    >();
    for (const [nodeId, nodePartitions] of Object.entries(partitionsByBroker)) {
      seekEntriesByBroker.set(nodeId, {
        topic,
        partitions: parsedPartitions.filter((entry) => nodePartitions.includes(entry.partition)),
      });
    }

    await retrier(retry)(async () => {
      try {
        const partitionErrors: {
          partition: number;
          offset: unknown;
          error: { retriable?: boolean; name?: string; type?: string };
        }[] = [];

        const requests = [...seekEntriesByBroker.entries()].map(
          ([nodeId, { topic: topicName, partitions: nodePartitions }]) =>
            (async () => {
              const broker = await cluster.findBroker({ nodeId });
              await broker.deleteRecords({ topics: [{ topic: topicName, partitions: nodePartitions }] });
              seekEntriesByBroker.delete(nodeId);
            })(),
        );

        await Promise.all(
          requests.map((request) =>
            request.catch((error: unknown) => {
              if (error instanceof KafkaDeleteTopicRecordsError) {
                for (const { partition, offset, error: partitionError } of error.partitions) {
                  partitionErrors.push({
                    partition,
                    offset,
                    error: partitionError ?? {},
                  });
                }
                return;
              }

              throw error;
            }),
          ),
        );

        if (partitionErrors.length > 0) {
          throw new KafkaDeleteTopicRecordsError({ partitions: partitionErrors });
        }
      } catch (error) {
        if (
          error instanceof KafkaDeleteTopicRecordsError &&
          error.retriable &&
          error.partitions.some(({ error: partitionError }) => {
            if (partitionError == null) return false;
            const descriptor = partitionError as { type?: string; name?: string };
            return staleMetadata(descriptor) || descriptor.name === 'KafkaMetadataNotLoaded';
          })
        ) {
          await cluster.refreshMetadata();
        }
        throw error;
      }
    });
  };

  return {
    listTopics,
    createTopics,
    deleteTopics,
    createPartitions,
    fetchTopicMetadata,
    describeCluster,
    deleteTopicRecords,
  };
}
