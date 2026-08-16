import { createErrorFromCode, failure } from '../../error-codes';

/**
 * Shared Metadata error check: topic-level error first, then each partition's.
 * Each response version keeps its own body type and calls this helper.
 */
export interface TopicMetadataErrorShape {
  topicErrorCode: number;
  partitionMetadata: readonly { partitionErrorCode: number }[];
}

export function checkTopicMetadataErrors(topicMetadata: readonly TopicMetadataErrorShape[]): void {
  const topicWithError = topicMetadata.find((topic) => failure(topic.topicErrorCode));
  if (topicWithError) throw createErrorFromCode(topicWithError.topicErrorCode);

  for (const topic of topicMetadata) {
    const partitionWithError = topic.partitionMetadata.find((partition) => failure(partition.partitionErrorCode));
    if (partitionWithError) throw createErrorFromCode(partitionWithError.partitionErrorCode);
  }
}
