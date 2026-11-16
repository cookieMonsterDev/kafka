import {
  KafkaAggregateError,
  KafkaBrokerNotFound,
  KafkaDeleteTopicRecordsError,
  KafkaNonRetriableError,
} from '../errors';
import { isRequestTimedOut, isTopicAlreadyExists, STALE_METADATA_ERRORS } from '../cluster/error-classifier';
import { CONTROLLER } from '../cluster/run';
import { createErrorFromCode } from '../protocol/error-codes';
import { API_KEYS } from '../protocol/requests/api-keys';
import { retrier as createRetrier } from '../retry/index';
import { parseOffset } from '../consumer/types';
import type { AdminContext } from './helpers';
import { requireMetadata, retryOnLeaderNotAvailable, formatUnknown } from './helpers';
import type {
  DescribeTopicPartitionsOptions,
  DescribeTopicPartitionsResult,
  DescribeTopicPartitionsTopic,
  SeekInput,
  TopicConfig,
  TopicMetadata,
  TopicOffset,
  TopicPartitionConfig,
} from './types';
import {
  CreatePartitions,
  CreateTopics,
  DeleteRecords,
  DeleteTopics,
  DescribeCluster,
  DescribeTopicPartitions,
} from '../protocol/requests/lazy-families';
import { Metadata } from '../protocol/requests/metadata/index';

const NO_CONTROLLER_ID = -1;
const LEADER_NOT_AVAILABLE_CODE = 5;

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
  describeTopicPartitions: (options: DescribeTopicPartitionsOptions) => Promise<DescribeTopicPartitionsResult>;
  deleteTopicRecords: (options: { topic: string; partitions: SeekInput[] }) => Promise<void>;
}

export function createTopicsApi(
  { cluster, logger, retry }: AdminContext,
  { fetchTopicOffsets }: { fetchTopicOffsets: (topic: string) => Promise<TopicOffset[]> },
): TopicsApi {
  const retrier = createRetrier(retry);

  const listTopics = async (): Promise<string[]> => {
    const { topicMetadata } = await requireMetadata(cluster);
    return topicMetadata.flatMap((topic) => (topic.topic == null ? [] : [topic.topic]));
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

    return cluster.run(
      CONTROLLER,
      async (broker) => {
        try {
          await broker.send(CreateTopics, { topics, validateOnly, timeout });
        } catch (error) {
          if (error instanceof KafkaAggregateError && error.errors.every(isTopicAlreadyExists)) {
            return false;
          }
          throw error;
        }

        // `validateOnly` never actually creates the topic, so its partitions never get a leader —
        // waiting for one here would just run out the clock on every dry-run call.
        if (waitForLeaders && !validateOnly) {
          const topicNamesArray = [...topicNames];
          await retryOnLeaderNotAvailable(
            async () =>
              broker.send(Metadata, { topics: topicNamesArray, allowAutoTopicCreation: broker.allowAutoTopicCreation }),
            {
              delay: 100,
              maxWait: timeout,
              timeoutMessage: 'Timed out while waiting for topic leaders',
            },
          );
        }

        return true;
      },
      { retrier, refreshMetadata: true, description: 'Could not create topics' },
    );
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

    // A topic created moments earlier may not have reached this broker's metadata yet: retry the
    // whole stale-metadata class (UNKNOWN_TOPIC_OR_PARTITION and friends), the same set
    // retryOnLeaderNotAvailable() retries on right after topic creation.
    await cluster.run(
      CONTROLLER,
      async (broker) => {
        await broker.send(CreatePartitions, { topicPartitions, validateOnly, timeout });
      },
      { retrier, refreshMetadata: true, retryOn: STALE_METADATA_ERRORS, description: 'Could not create partitions' },
    );
  };

  const deleteTopics = async ({ topics, timeout }: { topics: string[]; timeout?: number }): Promise<void> => {
    if (!topics || !Array.isArray(topics)) {
      throw new KafkaNonRetriableError(`Invalid topics array ${formatUnknown(topics)}`);
    }

    if (topics.filter((topic) => typeof topic !== 'string').length > 0) {
      throw new KafkaNonRetriableError('Invalid topics array, the names must be a valid string');
    }

    await cluster.run(
      CONTROLLER,
      async (broker) => {
        try {
          await broker.send(DeleteTopics, { topics, timeout });
        } catch (error) {
          if (isRequestTimedOut(error)) {
            logger.error(
              'Could not delete topics, check if "delete.topic.enable" is set to "true" (the default value is "false") or increase the timeout',
              { error: error instanceof Error ? error.message : String(error) },
            );
          }
          throw error;
        }

        for (const topic of topics) {
          cluster.targetTopics.delete(topic);
        }

        await cluster.refreshMetadata();
      },
      { retrier, refreshMetadata: true, retryOn: STALE_METADATA_ERRORS, description: 'Could not delete topics' },
    );
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
      topics: metadata.topicMetadata.flatMap((topicMetadata) => {
        if (topicMetadata.topic == null) return [];
        return [
          {
            name: topicMetadata.topic,
            ...(topicMetadata.topicId != null ? { topicId: topicMetadata.topicId } : {}),
            partitions: topicMetadata.partitionMetadata,
          },
        ];
      }),
    };
  };

  const describeCluster = async () => {
    await cluster.refreshMetadata();
    const versions = cluster.brokerPool.versions;
    if (versions?.[API_KEYS.DescribeCluster]) {
      const broker = await cluster.findControllerBroker();
      const body = await broker.send(DescribeCluster, {});
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

  const describeTopicPartitions = async ({
    topics,
    responsePartitionLimit,
    cursor,
  }: DescribeTopicPartitionsOptions): Promise<DescribeTopicPartitionsResult> => {
    if (!Array.isArray(topics)) {
      throw new KafkaNonRetriableError(`Invalid topics array ${formatUnknown(topics)}`);
    }

    const names = topics.map((entry, index) => {
      if (typeof entry === 'string') {
        if (entry.length === 0) {
          throw new KafkaNonRetriableError(`Invalid topic name ${formatUnknown(entry)}`);
        }
        return entry;
      }
      if (typeof entry !== 'object' || entry == null || typeof entry.topic !== 'string' || entry.topic.length === 0) {
        throw new KafkaNonRetriableError(`Invalid topics array entry at index ${index}`);
      }
      if (entry.topicId != null && (!Buffer.isBuffer(entry.topicId) || entry.topicId.length !== 16)) {
        throw new KafkaNonRetriableError(`Invalid topicId for topic "${entry.topic}", must be a 16-byte Buffer`);
      }
      return entry.topic;
    });

    if (responsePartitionLimit != null && (!Number.isInteger(responsePartitionLimit) || responsePartitionLimit < 1)) {
      throw new KafkaNonRetriableError(`Invalid responsePartitionLimit ${formatUnknown(responsePartitionLimit)}`);
    }

    if (cursor != null) {
      if (typeof cursor.topic !== 'string' || cursor.topic.length === 0) {
        throw new KafkaNonRetriableError(`Invalid cursor topic ${formatUnknown(cursor.topic)}`);
      }
      if (!Number.isInteger(cursor.partitionIndex) || cursor.partitionIndex < 0) {
        throw new KafkaNonRetriableError(`Invalid cursor partitionIndex ${formatUnknown(cursor.partitionIndex)}`);
      }
    }

    return cluster.run(
      CONTROLLER,
      async (broker) => {
        const body = await broker.send(DescribeTopicPartitions, {
          topics: names.map((topic) => ({ topic })),
          responsePartitionLimit,
          cursor: cursor ?? null,
        });

        return {
          topics: body.topics.map(
            ({ topic, topicId, isInternal, partitions, topicAuthorizedOperations }): DescribeTopicPartitionsTopic => ({
              name: topic,
              topicId,
              isInternal,
              topicAuthorizedOperations,
              partitions: partitions.map(
                ({
                  partitionIndex,
                  leader,
                  leaderEpoch,
                  replicas,
                  isr,
                  eligibleLeaderReplicas,
                  lastKnownElr,
                  offlineReplicas,
                }) => ({
                  partitionIndex,
                  leader,
                  leaderEpoch,
                  replicas,
                  isr,
                  eligibleLeaderReplicas,
                  lastKnownElr,
                  offlineReplicas,
                }),
              ),
            }),
          ),
          nextCursor: body.nextCursor,
        };
      },
      { retrier, refreshMetadata: true, description: 'Could not describe topic partitions' },
    );
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

    let pendingPartitions = parsedPartitions;

    // The leaders are looked up from metadata on every attempt, so `run` resolves no target. A
    // retriable per-partition error retries the partitions still pending, after a metadata refresh
    // when one of them names stale metadata.
    await cluster.run(
      () => [],
      async () => {
        const partitionErrors: {
          partition: number;
          offset: unknown;
          error: { retriable?: boolean; name?: string; type?: string };
        }[] = [];

        const leadersByBroker = cluster.findLeaderForPartitions(
          topic,
          pendingPartitions.map(({ partition }) => partition),
        );
        const routedPartitions = Object.values(leadersByBroker).flat();
        for (const { partition, offset } of pendingPartitions) {
          if (!routedPartitions.includes(partition)) {
            partitionErrors.push({ partition, offset, error: createErrorFromCode(LEADER_NOT_AVAILABLE_CODE) });
          }
        }

        const requests = Object.entries(leadersByBroker).map(([nodeId, nodePartitionIds]) =>
          (async () => {
            const broker = await cluster.findBroker({ nodeId });
            await broker.send(DeleteRecords, {
              topics: [
                {
                  topic,
                  partitions: pendingPartitions.filter(({ partition }) => nodePartitionIds.includes(partition)),
                },
              ],
            });
            pendingPartitions = pendingPartitions.filter(({ partition }) => !nodePartitionIds.includes(partition));
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
      },
      { retrier, retryRetriable: true },
    );
  };

  return {
    listTopics,
    createTopics,
    deleteTopics,
    createPartitions,
    fetchTopicMetadata,
    describeCluster,
    describeTopicPartitions,
    deleteTopicRecords,
  };
}
