import { KafkaBrokerNotFound, KafkaNonRetriableError } from '../errors';
import { retrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown, requireMetadata } from './helpers';
import type { DescribeProducersOptions, PartitionProducerState, TopicPartitions } from './types';

export interface ProducersApi {
  describeProducers: (options: DescribeProducersOptions) => Promise<PartitionProducerState[]>;
}

function validateTopicPartitions(topicPartitions: TopicPartitions[]): void {
  if (!Array.isArray(topicPartitions) || topicPartitions.length === 0) {
    throw new KafkaNonRetriableError(`Invalid topicPartitions array ${formatUnknown(topicPartitions)}`);
  }

  if (topicPartitions.some(({ topic }) => typeof topic !== 'string' || topic.length === 0)) {
    throw new KafkaNonRetriableError('Invalid topicPartitions array, topic names must be non-empty strings');
  }

  const names = new Set(topicPartitions.map(({ topic }) => topic));
  if (names.size !== topicPartitions.length) {
    throw new KafkaNonRetriableError('Invalid topicPartitions array, it cannot contain duplicate topics');
  }

  for (const { topic, partitions } of topicPartitions) {
    if (
      !Array.isArray(partitions) ||
      partitions.length === 0 ||
      partitions.some((partition) => !Number.isInteger(partition) || partition < 0)
    ) {
      throw new KafkaNonRetriableError(`Invalid partitions array for topic "${topic}"`);
    }
    if (new Set(partitions).size !== partitions.length) {
      throw new KafkaNonRetriableError(`Invalid partitions array for topic "${topic}", it contains duplicates`);
    }
  }
}

export function createProducersApi({ cluster, retry }: AdminContext): ProducersApi {
  const describeProducers = async ({
    topicPartitions,
    brokerId,
  }: DescribeProducersOptions): Promise<PartitionProducerState[]> => {
    validateTopicPartitions(topicPartitions);

    if (brokerId != null && !['string', 'number'].includes(typeof brokerId)) {
      throw new KafkaNonRetriableError(`Invalid brokerId ${formatUnknown(brokerId)}`);
    }

    return retrier(retry)(async () => {
      const requestsByBroker = new Map<string, Map<string, number[]>>();

      if (brokerId != null) {
        requestsByBroker.set(
          String(brokerId),
          new Map(topicPartitions.map(({ topic, partitions }) => [topic, partitions])),
        );
      } else {
        await requireMetadata(cluster, { topics: topicPartitions.map(({ topic }) => topic) });

        for (const { topic, partitions } of topicPartitions) {
          const partitionsByLeader = cluster.findLeaderForPartitions(topic, partitions);
          const foundPartitions = new Set(Object.values(partitionsByLeader).flat());
          const missing = partitions.filter((partition) => !foundPartitions.has(partition));
          if (missing.length > 0) {
            throw new KafkaBrokerNotFound(`Could not find leaders for ${topic} partitions ${missing.join(', ')}`, {
              retriable: true,
            });
          }

          for (const [nodeId, leaderPartitions] of Object.entries(partitionsByLeader)) {
            const topics = requestsByBroker.get(nodeId) ?? new Map<string, number[]>();
            topics.set(topic, leaderPartitions);
            requestsByBroker.set(nodeId, topics);
          }
        }
      }

      const responses = await Promise.all(
        [...requestsByBroker].map(async ([nodeId, topics]) => {
          const broker = await cluster.findBroker({ nodeId });
          return broker.describeProducers({
            topics: [...topics].map(([topic, partitions]) => ({ topic, partitions })),
          });
        }),
      );

      const states = new Map<string, PartitionProducerState>();
      for (const { topics } of responses) {
        for (const { topic, partitions } of topics) {
          for (const { partition, activeProducers } of partitions) {
            states.set(`${topic}\0${partition}`, {
              topic,
              partition,
              activeProducers: activeProducers.map(({ currentTransactionStartOffset, ...producer }) => ({
                ...producer,
                currentTransactionStartOffset:
                  currentTransactionStartOffset === -1n ? null : currentTransactionStartOffset,
              })),
            });
          }
        }
      }

      return topicPartitions.flatMap(({ topic, partitions }) =>
        partitions.map((partition) => {
          const state = states.get(`${topic}\0${partition}`);
          if (!state) {
            throw new KafkaNonRetriableError(`Broker omitted producer state for ${topic} partition ${partition}`);
          }
          return state;
        }),
      );
    });
  };

  return { describeProducers };
}
