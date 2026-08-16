import { createErrorFromCode, failure } from '../../error-codes';

export const REPLICA_ID = -1;

export interface ListOffsetsPartitionOptions {
  partition: number;
  /**
   * Use -1n for the latest offset, -2n for the earliest, -3n for the max timestamp
   * (ListOffsets v7+, KIP-734). Defaults to -1n.
   */
  timestamp?: bigint;
  /** ListOffsets v0 only; how many offsets to return. Defaults to 1. */
  maxNumOffsets?: number;
  /**
   * ListOffsets v4+ (KIP-320). Compared with the broker's current leader epoch.
   * Defaults to -1 (unknown / skip the check). Not written on v0–v3.
   */
  currentLeaderEpoch?: number;
}

export interface ListOffsetsTopicOptions {
  topic: string;
  partitions: ListOffsetsPartitionOptions[];
}

/**
 * Default each partition's `timestamp` to `-1n` (latest) and `currentLeaderEpoch` to `-1`
 * when omitted. v0–v3 request schemas ignore `currentLeaderEpoch`; v4+ write it.
 */
export function withDefaultTimestamps(
  topics: readonly ListOffsetsTopicOptions[],
): { topic: string; partitions: { partition: number; timestamp: bigint; currentLeaderEpoch: number }[] }[] {
  return topics.map(({ topic, partitions }) => ({
    topic,
    partitions: partitions.map(({ partition, timestamp = -1n, currentLeaderEpoch = -1 }) => ({
      partition,
      timestamp,
      currentLeaderEpoch,
    })),
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
