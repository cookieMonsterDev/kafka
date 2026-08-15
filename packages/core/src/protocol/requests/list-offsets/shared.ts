import { createErrorFromCode, failure } from '../../error-codes.js';

export const REPLICA_ID = -1;

export interface ListOffsetsPartitionOptions {
  partition: number;
  /** Use -1n for the latest offset, -2n for the earliest. Defaults to -1n. */
  timestamp?: bigint;
}

export interface ListOffsetsTopicOptions {
  topic: string;
  partitions: ListOffsetsPartitionOptions[];
}

/**
 * Every version's request body defaults each partition's `timestamp` to -1n (latest) when
 * omitted, same as kafkajs's own per-partition default — applied here since the schema itself
 * has no notion of field defaults.
 */
export function withDefaultTimestamps(
  topics: readonly ListOffsetsTopicOptions[],
): { topic: string; partitions: { partition: number; timestamp: bigint }[] }[] {
  return topics.map(({ topic, partitions }) => ({
    topic,
    partitions: partitions.map(({ partition, timestamp = -1n }) => ({ partition, timestamp })),
  }));
}

/**
 * Every ListOffsets response version shares this shape: a list of per-topic responses, each
 * carrying per-partition results with their own error code. There is no topic-level error code —
 * only the first failing partition (in topic, then partition, order) is surfaced.
 */
export interface ListOffsetsResponseShape {
  responses: readonly { partitions: readonly { errorCode: number }[] }[];
}

export function checkListOffsetsErrors(data: ListOffsetsResponseShape): void {
  for (const response of data.responses) {
    const partitionWithError = response.partitions.find((partition) => failure(partition.errorCode));
    if (partitionWithError) throw createErrorFromCode(partitionWithError.errorCode);
  }
}
