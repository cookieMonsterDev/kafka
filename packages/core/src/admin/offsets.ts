import { EARLIEST_OFFSET, LATEST_OFFSET } from '../constants';
import { createConsumer } from '../consumer/index';
import { parseOffset } from '../consumer/types';
import { KafkaNonRetriableError, KafkaTimeout } from '../errors';
import { LOG_LEVELS } from '../loggers/index';
import { STALE_METADATA_ERRORS } from '../cluster/error-classifier';
import { retrier as createRetrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { findTopicPartitions, isConsumerGroupIdle } from './helpers';
import type { FetchOffsetsPartition, SeekEntry, SeekInput, TopicOffset } from './types';
import { OffsetFetch } from '../protocol/requests/offset-fetch/index';

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
  const retrier = createRetrier(retry);

  const fetchTopicOffsets = async (topic: string): Promise<TopicOffset[]> => {
    if (!topic || typeof topic !== 'string') {
      throw new KafkaNonRetriableError(`Invalid topic ${topic}`);
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

    const coordinator = await cluster.findGroupCoordinator({ groupId });
    const topicsToFetch = await Promise.all(
      topicsToQuery.map(async (topic) => {
        const partitions = await findTopicPartitions(cluster, topic);
        return { topic, partitions: partitions.map((partition) => ({ partition })) };
      }),
    );

    let { responses: consumerOffsets } = await coordinator.send(OffsetFetch, {
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
