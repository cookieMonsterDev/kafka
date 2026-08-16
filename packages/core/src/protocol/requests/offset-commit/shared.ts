import { createErrorFromCode, failure } from '../../error-codes';

/** Signals to the broker that its default retention configuration should be used. */
export const RETENTION_TIME = -1n;

export interface OffsetCommitPartitionOptions {
  partition: number;
  offset: bigint;
  /** OffsetCommit v1 only; defaults to `Date.now()` when omitted. */
  timestamp?: bigint;
  metadata?: string | null;
}

export interface OffsetCommitTopicOptions {
  topic: string;
  partitions: OffsetCommitPartitionOptions[];
}

/** Every version defaults a partition's `metadata` to null when omitted. */
export function withDefaultMetadata(
  topics: readonly OffsetCommitTopicOptions[],
): { topic: string; partitions: { partition: number; offset: bigint; metadata: string | null }[] }[] {
  return topics.map(({ topic, partitions }) => ({
    topic,
    partitions: partitions.map(({ partition, offset, metadata = null }) => ({ partition, offset, metadata })),
  }));
}

/** OffsetCommit v1 also writes a per-partition timestamp (defaults to now). */
export function withDefaultMetadataAndTimestamp(topics: readonly OffsetCommitTopicOptions[]): {
  topic: string;
  partitions: { partition: number; offset: bigint; timestamp: bigint; metadata: string | null }[];
}[] {
  const now = BigInt(Date.now());
  return topics.map(({ topic, partitions }) => ({
    topic,
    partitions: partitions.map(({ partition, offset, timestamp = now, metadata = null }) => ({
      partition,
      offset,
      timestamp,
      metadata,
    })),
  }));
}

export interface OffsetCommitResponseShape {
  responses: readonly { partitions: readonly { errorCode: number }[] }[];
}

export function checkOffsetCommitErrors(data: OffsetCommitResponseShape): void {
  for (const response of data.responses) {
    const partitionWithError = response.partitions.find((partition) => failure(partition.errorCode));
    if (partitionWithError) throw createErrorFromCode(partitionWithError.errorCode);
  }
}
