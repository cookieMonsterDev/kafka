import { createErrorFromCode, failure } from '../../error-codes.js'

/**
 * Every Metadata response version shares this error-checking shape (topic-level error code, then
 * each partition's own), even though the surrounding body grows extra fields release over
 * release. kafkajs expresses the reuse by literally re-exporting `parse` from v0's module; here
 * each version keeps its own precise body type and calls this shared check instead, so `parse`'s
 * return type is never widened down to v0's.
 */
export interface TopicMetadataErrorShape {
  topicErrorCode: number
  partitionMetadata: readonly { partitionErrorCode: number }[]
}

export function checkTopicMetadataErrors(topicMetadata: readonly TopicMetadataErrorShape[]): void {
  const topicWithError = topicMetadata.find((topic) => failure(topic.topicErrorCode))
  if (topicWithError) throw createErrorFromCode(topicWithError.topicErrorCode)

  for (const topic of topicMetadata) {
    const partitionWithError = topic.partitionMetadata.find((partition) => failure(partition.partitionErrorCode))
    if (partitionWithError) throw createErrorFromCode(partitionWithError.partitionErrorCode)
  }
}
