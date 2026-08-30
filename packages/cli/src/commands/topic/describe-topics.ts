import type { Admin, DescribeTopicPartitionsResult } from '@cookiemonsterdev/kafka-core';

export interface DescribedTopicPartition {
  readonly partitionIndex: number;
  readonly leader: number;
  readonly replicas: readonly number[];
  readonly isr: readonly number[];
}

export interface DescribedTopic {
  readonly name: string | null;
  readonly topicId?: Buffer;
  readonly partitions: readonly DescribedTopicPartition[];
}

/** Structural shape of `Admin.fetchTopicMetadata`'s result — narrow enough to normalize without a value import. */
interface FetchTopicMetadataResult {
  readonly topics: readonly {
    readonly name: string;
    readonly topicId?: Buffer;
    readonly partitions: readonly { partitionId: number; leader: number; replicas: number[]; isr: number[] }[];
  }[];
}

export function normalizeDescribeTopicPartitionsResult(result: DescribeTopicPartitionsResult): DescribedTopic[] {
  return result.topics.map((topic) => ({
    name: topic.name,
    topicId: topic.topicId,
    partitions: topic.partitions.map((partition) => ({
      partitionIndex: partition.partitionIndex,
      leader: partition.leader,
      replicas: partition.replicas,
      isr: partition.isr,
    })),
  }));
}

export function normalizeFetchTopicMetadataResult(result: FetchTopicMetadataResult): DescribedTopic[] {
  return result.topics.map((topic) => ({
    name: topic.name,
    topicId: topic.topicId,
    partitions: topic.partitions.map((partition) => ({
      partitionIndex: partition.partitionId,
      leader: partition.leader,
      replicas: partition.replicas,
      isr: partition.isr,
    })),
  }));
}

function isUnsupportedApiKeyError(error: unknown): boolean {
  return (
    typeof error === 'object' && error !== null && 'name' in error && error.name === 'KafkaServerDoesNotSupportApiKey'
  );
}

/**
 * Prefers `describeTopicPartitions` (KIP-966, richer per-partition detail); falls back to
 * `fetchTopicMetadata` only when the broker is too old to support it. The branch itself is a
 * pure function of that one named error, so both paths are unit-tested without a broker.
 */
export async function describeTopics(admin: Admin, topics: readonly string[]): Promise<DescribedTopic[]> {
  try {
    const result = await admin.describeTopicPartitions({ topics: [...topics] });
    return normalizeDescribeTopicPartitionsResult(result);
  } catch (error) {
    if (!isUnsupportedApiKeyError(error)) throw error;
    const result = await admin.fetchTopicMetadata({ topics: [...topics] });
    return normalizeFetchTopicMetadataResult(result);
  }
}
