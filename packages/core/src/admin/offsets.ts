import type { Broker } from '../broker/index';
import { supportsBatchedOffsetFetch } from '../broker/capabilities';
import { EARLIEST_OFFSET, LATEST_OFFSET } from '../constants';
import { createConsumer } from '../consumer/index';
import { parseOffset } from '../consumer/types';
import { KafkaNonRetriableError, KafkaTimeout } from '../errors';
import { LOG_LEVELS } from '../loggers/index';
import { STALE_METADATA_ERRORS } from '../cluster/error-classifier';
import { coordinatorTarget } from '../cluster/run';
import { retrier as createRetrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { findTopicPartitions, formatUnknown, groupByBroker, isConsumerGroupIdle } from './helpers';
import type {
  ConsumerGroupOffsets,
  FetchOffsetsPartition,
  FetchTopicOffsetsOptions,
  ListConsumerGroupOffsetsOptions,
  SeekEntry,
  SeekInput,
  TopicOffset,
} from './types';
import {
  committedOffsetsOf,
  OffsetFetch,
  type OffsetFetchGroupOptions,
  type OffsetFetchResponse,
  type OffsetFetchTopicResult,
} from '../protocol/requests/offset-fetch/index';
import { OFFSET_SPECS } from '../protocol/enums/offset-specs';

export interface OffsetsApi {
  fetchTopicOffsets: (topic: string, options?: FetchTopicOffsetsOptions) => Promise<TopicOffset[]>;
  fetchTopicOffsetsByTimestamp: (topic: string, timestamp?: bigint | number | string) => Promise<SeekEntry[]>;
  fetchOffsets: (options: {
    groupId: string;
    topics?: string[];
    resolveOffsets?: boolean;
  }) => Promise<{ topic: string; partitions: FetchOffsetsPartition[] }[]>;
  listConsumerGroupOffsets: (options: ListConsumerGroupOffsetsOptions) => Promise<{ groups: ConsumerGroupOffsets[] }>;
  resetOffsets: (options: { groupId: string; topic: string; earliest?: boolean }) => Promise<void>;
  setOffsets: (options: { groupId: string; topic: string; partitions: SeekInput[] }) => Promise<void>;
}

/** One consumer group's offset lookup; no topics looks up every topic it has committed offsets for. */
interface GroupOffsetsQuery {
  groupId: string;
  topics: readonly string[];
}

/**
 * Sends one OffsetFetch for all of `groups` to a coordinator on Kafka 3.0+ (v8, KIP-709), and one
 * request per group to an older one.
 */
async function sendOffsetFetch(
  coordinator: Broker,
  groups: readonly OffsetFetchGroupOptions[],
): Promise<OffsetFetchResponse['groups']> {
  if (coordinator.versions != null && supportsBatchedOffsetFetch(coordinator.versions)) {
    const response = await coordinator.send(OffsetFetch, { groups });
    return response.groups;
  }
  const responses = await Promise.all(groups.map((group) => coordinator.send(OffsetFetch, { groups: [group] })));
  return responses.flatMap((response) => response.groups);
}

function toCommittedTopicOffsets(topics: readonly OffsetFetchTopicResult[]): ConsumerGroupOffsets['topics'] {
  return topics.map(({ topic, partitions }) => ({
    topic,
    partitions: partitions.map(({ partition, offset, metadata }) => ({
      partition,
      offset,
      metadata: metadata || null,
    })),
  }));
}

function assertValidGroupQueries(groups: unknown): asserts groups is ListConsumerGroupOffsetsOptions['groups'] {
  if (!Array.isArray(groups) || groups.length === 0) {
    throw new KafkaNonRetriableError(`Invalid groups array ${formatUnknown(groups)}`);
  }
  const seenGroupIds = new Set<string>();
  for (const { groupId, topics } of groups as { groupId?: unknown; topics?: unknown }[]) {
    if (typeof groupId !== 'string' || groupId === '') {
      throw new KafkaNonRetriableError('Each group must have a non-empty groupId');
    }
    if (seenGroupIds.has(groupId)) {
      throw new KafkaNonRetriableError(`Duplicate groupId ${groupId}`);
    }
    seenGroupIds.add(groupId);
    if (topics != null && (!Array.isArray(topics) || topics.some((topic) => typeof topic !== 'string' || !topic))) {
      throw new KafkaNonRetriableError(`Invalid topics array for group ${groupId}`);
    }
  }
}

const OFFSET_SPEC_VALUES: ReadonlySet<bigint> = new Set(Object.values(OFFSET_SPECS));

function toTimestamp(timestamp: bigint | number | string | undefined): bigint | undefined {
  if (timestamp == null) return undefined;
  if (typeof timestamp === 'bigint') return timestamp;
  return parseOffset(timestamp);
}

export function createOffsetsApi({ cluster, rootLogger, retry }: AdminContext): OffsetsApi {
  const retrier = createRetrier(retry);

  /**
   * The committed offsets of each query's group, in `queries` order. Coordinators are resolved
   * together, and topic ids are read on every attempt, so a retry after UNKNOWN_TOPIC_ID sends the
   * ids of the refreshed metadata.
   */
  const fetchCommittedOffsets = async (queries: readonly GroupOffsetsQuery[]): Promise<OffsetFetchTopicResult[][]> => {
    const distinctTopics = [...new Set(queries.flatMap(({ topics }) => topics))];
    if (distinctTopics.length > 0) {
      await cluster.addMultipleTargetTopics(distinctTopics);
      await cluster.refreshMetadataIfNecessary();
    }
    const partitionsByTopic = new Map(
      distinctTopics.map((topic) => [
        topic,
        cluster.findTopicPartitionMetadata(topic).map(({ partitionId }) => ({ partition: partitionId })),
      ]),
    );

    const response = await cluster.run(
      () => queries.map(({ groupId }) => coordinatorTarget(groupId)),
      async (coordinators) => {
        const groups = queries.map(({ groupId, topics }) => ({
          groupId,
          topics: topics.map((topic) => ({
            topic,
            topicId: cluster.findTopicId(topic),
            partitions: partitionsByTopic.get(topic) ?? [],
          })),
        }));
        const fetched = await Promise.all(
          [...groupByBroker(groups, coordinators)].map(([coordinator, coordinatorGroups]) =>
            sendOffsetFetch(coordinator, coordinatorGroups),
          ),
        );
        return { groups: fetched.flat() };
      },
      { retrier, retryRetriable: true },
    );

    return queries.map(({ groupId }) => committedOffsetsOf(response, groupId));
  };

  const fetchTopicOffsets = async (
    topic: string,
    { offsetSpec = OFFSET_SPECS.LATEST }: FetchTopicOffsetsOptions = {},
  ): Promise<TopicOffset[]> => {
    if (!topic || typeof topic !== 'string') {
      throw new KafkaNonRetriableError(`Invalid topic ${topic}`);
    }
    if (!OFFSET_SPEC_VALUES.has(offsetSpec)) {
      throw new KafkaNonRetriableError(`Invalid offsetSpec ${String(offsetSpec)}`);
    }

    // The partition leaders are looked up inside, so `run` resolves no target; a topic the
    // metadata does not know yet is retried after a refresh.
    return cluster.run(
      () => [],
      async () => {
        await cluster.addTargetTopic(topic);
        await cluster.refreshMetadataIfNecessary();

        const metadata = cluster.findTopicPartitionMetadata(topic);
        const partitions = metadata.map((entry) => ({ partition: entry.partitionId }));

        // First, so a spec the broker is too old for fails before the watermark lookups.
        const isWatermark = offsetSpec === OFFSET_SPECS.LATEST || offsetSpec === OFFSET_SPECS.EARLIEST;
        const specOffsets = isWatermark
          ? undefined
          : await cluster.fetchTopicsOffset([{ topic, fromTimestamp: offsetSpec, partitions }]);
        const high = await cluster.fetchTopicsOffset([{ topic, fromBeginning: false, partitions }]);
        const low = await cluster.fetchTopicsOffset([{ topic, fromBeginning: true, partitions }]);

        const highTopic = high.pop();
        const lowTopic = low.pop();
        const highPartitions = highTopic?.partitions ?? [];
        const lowPartitions = lowTopic?.partitions ?? [];
        const watermarkPartitions = offsetSpec === OFFSET_SPECS.EARLIEST ? lowPartitions : highPartitions;
        const specPartitions = specOffsets === undefined ? watermarkPartitions : (specOffsets.pop()?.partitions ?? []);

        return highPartitions.map(({ partition, offset }) => ({
          partition,
          offset: specPartitions.find((entry) => entry.partition === partition)!.offset,
          high: offset,
          low: lowPartitions.find((entry) => entry.partition === partition)!.offset,
        }));
      },
      { retrier, retryOn: STALE_METADATA_ERRORS },
    );
  };

  const fetchTopicOffsetsByTimestamp = async (
    topic: string,
    timestamp?: bigint | number | string,
  ): Promise<SeekEntry[]> => {
    if (!topic || typeof topic !== 'string') {
      throw new KafkaNonRetriableError(`Invalid topic ${topic}`);
    }

    return cluster.run(
      () => [],
      async () => {
        await cluster.addTargetTopic(topic);
        await cluster.refreshMetadataIfNecessary();

        const metadata = cluster.findTopicPartitionMetadata(topic);
        const partitions = metadata.map((entry) => ({ partition: entry.partitionId }));

        const high = await cluster.fetchTopicsOffset([{ topic, fromBeginning: false, partitions }]);
        const highPartitions = high.pop()?.partitions ?? [];

        const offsets = await cluster.fetchTopicsOffset([{ topic, fromTimestamp: toTimestamp(timestamp), partitions }]);
        const timestampPartitions = offsets.pop()?.partitions ?? [];

        return timestampPartitions.map(({ partition, offset }) => ({
          partition,
          offset: offset >= 0n ? offset : highPartitions.find((entry) => entry.partition === partition)!.offset,
        }));
      },
      { retrier, retryOn: STALE_METADATA_ERRORS },
    );
  };

  const setOffsets = async ({
    groupId,
    topic,
    partitions,
  }: {
    groupId: string;
    topic: string;
    partitions: SeekInput[];
  }): Promise<void> => {
    if (!groupId) {
      throw new KafkaNonRetriableError(`Invalid groupId ${groupId}`);
    }

    if (!topic) {
      throw new KafkaNonRetriableError(`Invalid topic ${topic}`);
    }

    if (!partitions || partitions.length === 0) {
      throw new KafkaNonRetriableError('Invalid partitions');
    }

    const consumer = createConsumer({
      logger: rootLogger.namespace('Admin', LOG_LEVELS.NOTHING),
      cluster,
      groupId,
      maxWaitTimeInMs: 100,
    });

    await consumer.subscribe({ topic, fromBeginning: true });
    const description = await consumer.describeGroup();

    if (!isConsumerGroupIdle(description.state)) {
      throw new KafkaNonRetriableError(
        `The consumer group must have no running instances, current state: ${description.state}`,
      );
    }

    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        void consumer.stop().finally(() => {
          reject(new KafkaTimeout('Timeout while setting offsets'));
        });
      }, 30_000);

      const finish = (action: () => void): void => {
        clearTimeout(timeoutId);
        action();
      };

      consumer.on(consumer.events.FETCH, () => {
        finish(() => {
          void consumer.stop().then(resolve).catch(reject);
        });
      });

      consumer.on(consumer.events.CRASH, (event) => {
        finish(() => reject((event.payload as { error: Error }).error));
      });

      void consumer
        .run({
          eachBatchAutoResolve: false,
          eachBatch: async () => undefined,
        })
        .catch((error: unknown) => {
          finish(() => reject(error instanceof Error ? error : new Error(String(error))));
        });

      try {
        consumer.pause([{ topic }]);
        for (const { partition, offset } of partitions) {
          consumer.seek({ topic, partition, offset });
        }
      } catch (error) {
        finish(() => reject(error instanceof Error ? error : new Error(String(error))));
      }
    });
  };

  const resetOffsets = async ({
    groupId,
    topic,
    earliest = false,
  }: {
    groupId: string;
    topic: string;
    earliest?: boolean;
  }): Promise<void> => {
    if (!groupId) {
      throw new KafkaNonRetriableError(`Invalid groupId ${groupId}`);
    }

    if (!topic) {
      throw new KafkaNonRetriableError(`Invalid topic ${topic}`);
    }

    const partitions = await findTopicPartitions(cluster, topic);
    const partitionsToSeek = partitions.map((partition) => ({
      partition,
      offset: cluster.defaultOffset({ fromBeginning: earliest }),
    }));

    return setOffsets({ groupId, topic, partitions: partitionsToSeek });
  };

  const fetchOffsets = async ({
    groupId,
    topics,
    resolveOffsets = false,
  }: {
    groupId: string;
    topics?: string[];
    resolveOffsets?: boolean;
  }): Promise<{ topic: string; partitions: FetchOffsetsPartition[] }[]> => {
    if (!groupId) {
      throw new KafkaNonRetriableError(`Invalid groupId ${groupId}`);
    }

    const topicsToQuery = topics ?? [];
    if (!Array.isArray(topicsToQuery)) {
      throw new KafkaNonRetriableError('Expected topics array to be set');
    }

    let [consumerOffsets = []] = await fetchCommittedOffsets([{ groupId, topics: topicsToQuery }]);

    if (resolveOffsets) {
      consumerOffsets = await Promise.all(
        consumerOffsets.map(async ({ topic, partitions }) => {
          const topicOffsets = await fetchTopicOffsets(topic);
          const indexedOffsets = new Map(topicOffsets.map((entry) => [entry.partition, entry]));
          const recalculatedPartitions = partitions.map(({ offset, partition, ...props }) => {
            let resolvedOffset = offset;
            if (offset === BigInt(EARLIEST_OFFSET)) {
              resolvedOffset = indexedOffsets.get(partition)!.low;
            }
            if (offset === BigInt(LATEST_OFFSET)) {
              resolvedOffset = indexedOffsets.get(partition)!.high;
            }
            return { partition, offset: resolvedOffset, ...props };
          });

          await setOffsets({ groupId, topic, partitions: recalculatedPartitions });

          return { topic, partitions: recalculatedPartitions };
        }),
      );
    }

    return toCommittedTopicOffsets(consumerOffsets);
  };

  const listConsumerGroupOffsets = async ({
    groups,
  }: ListConsumerGroupOffsetsOptions): Promise<{ groups: ConsumerGroupOffsets[] }> => {
    assertValidGroupQueries(groups);

    const queries = groups.map(({ groupId, topics = [] }) => ({ groupId, topics }));
    const committedOffsets = await fetchCommittedOffsets(queries);
    return {
      groups: queries.map(({ groupId }, index) => ({
        groupId,
        topics: toCommittedTopicOffsets(committedOffsets[index] ?? []),
      })),
    };
  };

  return {
    fetchTopicOffsets,
    fetchTopicOffsetsByTimestamp,
    fetchOffsets,
    listConsumerGroupOffsets,
    resetOffsets,
    setOffsets,
  };
}
