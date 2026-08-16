import { EARLIEST_OFFSET, LATEST_OFFSET } from '../constants.js';
import { createConsumer } from '../consumer/index.js';
import { parseOffset } from '../consumer/types.js';
import { KafkaJSNonRetriableError } from '../errors.js';
import { LOG_LEVELS } from '../loggers/index.js';
import { retrier } from '../retry/index.js';
import type { AdminContext } from './helpers.js';
import { findTopicPartitions, isConsumerGroupIdle, protocolType } from './helpers.js';
import type { FetchOffsetsPartition, SeekEntry, SeekInput, TopicOffset } from './types.js';

export interface OffsetsApi {
  fetchTopicOffsets: (topic: string) => Promise<TopicOffset[]>;
  fetchTopicOffsetsByTimestamp: (topic: string, timestamp?: bigint | number | string) => Promise<SeekEntry[]>;
  fetchOffsets: (options: {
    groupId: string;
    topics?: string[];
    resolveOffsets?: boolean;
  }) => Promise<{ topic: string; partitions: FetchOffsetsPartition[] }[]>;
  resetOffsets: (options: { groupId: string; topic: string; earliest?: boolean }) => Promise<void>;
  setOffsets: (options: { groupId: string; topic: string; partitions: SeekInput[] }) => Promise<void>;
}

function toTimestamp(timestamp: bigint | number | string | undefined): bigint | undefined {
  if (timestamp == null) return undefined;
  if (typeof timestamp === 'bigint') return timestamp;
  return parseOffset(timestamp);
}

export function createOffsetsApi({ cluster, rootLogger, retry }: AdminContext): OffsetsApi {
  const fetchTopicOffsets = async (topic: string): Promise<TopicOffset[]> => {
    if (!topic || typeof topic !== 'string') {
      throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`);
    }

    return retrier(retry)(async (bail) => {
      try {
        await cluster.addTargetTopic(topic);
        await cluster.refreshMetadataIfNecessary();

        const metadata = cluster.findTopicPartitionMetadata(topic);
        const partitions = metadata.map((entry) => ({ partition: entry.partitionId }));

        const high = await cluster.fetchTopicsOffset([{ topic, fromBeginning: false, partitions }]);
        const low = await cluster.fetchTopicsOffset([{ topic, fromBeginning: true, partitions }]);

        const highTopic = high.pop();
        const lowTopic = low.pop();
        const highPartitions = highTopic?.partitions ?? [];
        const lowPartitions = lowTopic?.partitions ?? [];

        return highPartitions.map(({ partition, offset }) => ({
          partition,
          offset,
          high: offset,
          low: lowPartitions.find((entry) => entry.partition === partition)!.offset,
        }));
      } catch (error) {
        if (protocolType(error) === 'UNKNOWN_TOPIC_OR_PARTITION') {
          await cluster.refreshMetadata();
          throw error;
        }

        bail(error as Error);
        return [];
      }
    });
  };

  const fetchTopicOffsetsByTimestamp = async (
    topic: string,
    timestamp?: bigint | number | string,
  ): Promise<SeekEntry[]> => {
    if (!topic || typeof topic !== 'string') {
      throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`);
    }

    return retrier(retry)(async (bail) => {
      try {
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
      } catch (error) {
        if (protocolType(error) === 'UNKNOWN_TOPIC_OR_PARTITION') {
          await cluster.refreshMetadata();
          throw error;
        }

        bail(error as Error);
        return [];
      }
    });
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
      throw new KafkaJSNonRetriableError(`Invalid groupId ${groupId}`);
    }

    if (!topic) {
      throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`);
    }

    if (!partitions || partitions.length === 0) {
      throw new KafkaJSNonRetriableError('Invalid partitions');
    }

    const consumer = createConsumer({
      logger: rootLogger.namespace('Admin', LOG_LEVELS.NOTHING),
      cluster,
      groupId,
    });

    await consumer.subscribe({ topic, fromBeginning: true });
    const description = await consumer.describeGroup();

    if (!isConsumerGroupIdle(description.state)) {
      throw new KafkaJSNonRetriableError(
        `The consumer group must have no running instances, current state: ${description.state}`,
      );
    }

    return new Promise((resolve, reject) => {
      consumer.on(consumer.events.FETCH, () => {
        void consumer.stop().then(resolve).catch(reject);
      });

      void consumer
        .run({
          eachBatchAutoResolve: false,
          eachBatch: async () => undefined,
        })
        .catch(reject);

      consumer.pause([{ topic }]);

      for (const { partition, offset } of partitions) {
        consumer.seek({ topic, partition, offset });
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
      throw new KafkaJSNonRetriableError(`Invalid groupId ${groupId}`);
    }

    if (!topic) {
      throw new KafkaJSNonRetriableError(`Invalid topic ${topic}`);
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
      throw new KafkaJSNonRetriableError(`Invalid groupId ${groupId}`);
    }

    const topicsToQuery = topics ?? [];
    if (!Array.isArray(topicsToQuery)) {
      throw new KafkaJSNonRetriableError('Expected topics array to be set');
    }

    const coordinator = await cluster.findGroupCoordinator({ groupId });
    const topicsToFetch = await Promise.all(
      topicsToQuery.map(async (topic) => {
        const partitions = await findTopicPartitions(cluster, topic);
        return { topic, partitions: partitions.map((partition) => ({ partition })) };
      }),
    );

    let { responses: consumerOffsets } = await coordinator.offsetFetch({
      groupId,
      topics: topicsToFetch,
    });

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

    return consumerOffsets.map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.map(({ partition, offset, metadata }) => ({
        partition,
        offset,
        metadata: metadata || null,
      })),
    }));
  };

  return {
    fetchTopicOffsets,
    fetchTopicOffsetsByTimestamp,
    fetchOffsets,
    resetOffsets,
    setOffsets,
  };
}
