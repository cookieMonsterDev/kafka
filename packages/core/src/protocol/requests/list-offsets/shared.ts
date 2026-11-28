import { OFFSET_SPECS } from '../../enums/offset-specs';
import { createErrorFromCode, failure } from '../../error-codes';

export const REPLICA_ID = -1;

/**
 * The broker-side wait for a lookup that reads tiered storage (v10+, KIP-1075) when the caller
 * sets none: the client's default `requestTimeout`.
 */
export const DEFAULT_TIMEOUT_MS = 30_000;

export interface ListOffsetsPartitionOptions {
  partition: number;
  /** A timestamp in milliseconds, or one of `OFFSET_SPECS`. Defaults to `OFFSET_SPECS.LATEST`. */
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

/** The first version that reads each offset spec; an older one would read it as a timestamp. */
const FIRST_VERSION_BY_OFFSET_SPEC: ReadonlyMap<bigint, number> = new Map([
  [OFFSET_SPECS.MAX_TIMESTAMP, 7],
  [OFFSET_SPECS.EARLIEST_LOCAL, 8],
  [OFFSET_SPECS.LATEST_TIERED, 9],
  [OFFSET_SPECS.EARLIEST_PENDING_UPLOAD, 11],
]);

/** The version that reads every offset spec; from it on, no request needs the check. */
export const LATEST_OFFSET_SPEC_VERSION = Math.max(...FIRST_VERSION_BY_OFFSET_SPEC.values());

/** The lowest version that reads every offset spec in `topics`. */
export function minimumVersionForOffsetSpecs(topics: readonly ListOffsetsTopicOptions[]): number {
  let minimumVersion = 0;
  for (const { partitions } of topics) {
    for (const { timestamp } of partitions) {
      if (timestamp === undefined) continue;
      minimumVersion = Math.max(minimumVersion, FIRST_VERSION_BY_OFFSET_SPEC.get(timestamp) ?? 0);
    }
  }
  return minimumVersion;
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
    partitions: partitions.map(({ partition, timestamp = OFFSET_SPECS.LATEST, currentLeaderEpoch = -1 }) => ({
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
    partitions: partitions.map(({ partition, timestamp = OFFSET_SPECS.LATEST, maxNumOffsets = 1 }) => ({
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
