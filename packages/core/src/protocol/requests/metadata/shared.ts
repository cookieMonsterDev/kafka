import { createErrorFromCode, failure } from '../../error-codes';

/**
 * Java `Integer.MIN_VALUE`. Brokers send this for `cluster_authorized_operations` /
 * `topic_authorized_operations` when those bitmasks were not computed (include flags false).
 */
export const AUTHORIZED_OPERATIONS_OMITTED = -2147483648;

/**
 * Shared Metadata error check: topic-level error first, then each partition's.
 * Each response version keeps its own body type and calls this helper.
 */
export interface TopicMetadataErrorShape {
  topicErrorCode: number;
  topic?: string | null;
  partitionMetadata: readonly { partitionErrorCode: number; partitionId?: number }[];
}

export function checkTopicMetadataErrors(topicMetadata: readonly TopicMetadataErrorShape[]): void {
  const topicWithError = topicMetadata.find((topic) => failure(topic.topicErrorCode));
  if (topicWithError) {
    throw createErrorFromCode(topicWithError.topicErrorCode, { topic: topicWithError.topic ?? undefined });
  }

  for (const topic of topicMetadata) {
    const partitionWithError = topic.partitionMetadata.find((partition) => failure(partition.partitionErrorCode));
    if (partitionWithError) {
      throw createErrorFromCode(partitionWithError.partitionErrorCode, {
        topic: topic.topic ?? undefined,
        partition: partitionWithError.partitionId,
      });
    }
  }
}
