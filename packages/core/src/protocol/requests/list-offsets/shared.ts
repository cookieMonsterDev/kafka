import { createErrorFromCode, failure } from '../../error-codes';

export const REPLICA_ID = -1;

export interface ListOffsetsPartitionOptions {
  partition: number;
  /** Use -1n for the latest offset, -2n for the earliest. Defaults to -1n. */
  timestamp?: bigint;
  /** ListOffsets v0 only; how many offsets to return. Defaults to 1. */
  maxNumOffsets?: number;
}

export interface ListOffsetsTopicOptions {
  topic: string;
  partitions: ListOffsetsPartitionOptions[];
}

/** Default each partition's `timestamp` to `-1n` (latest) when omitted. */
export function withDefaultTimestamps(
  topics: readonly ListOffsetsTopicOptions[],
): { topic: string; partitions: { partition: number; timestamp: bigint }[] }[] {
  return topics.map(({ topic, partitions }) => ({
    topic,
    partitions: partitions.map(({ partition, timestamp = -1n }) => ({ partition, timestamp })),
  }));
}

/** ListOffsets v0 also writes `max_num_offsets` (defaults to 1). */
export function withDefaultTimestampsAndMaxOffsets(
  topics: readonly ListOffsetsTopicOptions[],
): { topic: string; partitions: { partition: number; timestamp: bigint; maxNumOffsets: number }[] }[] {
  return topics.map(({ topic, partitions }) => ({
    topic,
    partitions: partitions.map(({ partition, timestamp = -1n, maxNumOffsets = 1 }) => ({
      partition,
      timestamp,
      maxNumOffsets,
    })),
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
