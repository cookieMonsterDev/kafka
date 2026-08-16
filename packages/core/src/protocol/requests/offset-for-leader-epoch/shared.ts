import { createErrorFromCode, failure } from '../../error-codes';

/** Clients send replica_id -1 (not a replica). */
export const REPLICA_ID = -1;

/** Unknown / unset leader epoch on the wire. */
export const UNKNOWN_LEADER_EPOCH = -1;

export interface OffsetForLeaderEpochPartitionOptions {
  partition: number;
  currentLeaderEpoch?: number;
  leaderEpoch: number;
}

export interface OffsetForLeaderEpochTopicOptions {
  topic: string;
  partitions: OffsetForLeaderEpochPartitionOptions[];
}

/** v1+ partition list, filling `currentLeaderEpoch` with unknown (-1) when omitted. */
export function withCurrentLeaderEpochs(
  topics: readonly OffsetForLeaderEpochTopicOptions[],
): { topic: string; partitions: { partition: number; currentLeaderEpoch: number; leaderEpoch: number }[] }[] {
  return topics.map(({ topic, partitions }) => ({
    topic,
    partitions: partitions.map(({ partition, currentLeaderEpoch = UNKNOWN_LEADER_EPOCH, leaderEpoch }) => ({
      partition,
      currentLeaderEpoch,
      leaderEpoch,
    })),
  }));
}

/** v0 has no current_leader_epoch field. */
export function withoutCurrentLeaderEpoch(
  topics: readonly OffsetForLeaderEpochTopicOptions[],
): { topic: string; partitions: { partition: number; leaderEpoch: number }[] }[] {
  return topics.map(({ topic, partitions }) => ({
    topic,
    partitions: partitions.map(({ partition, leaderEpoch }) => ({ partition, leaderEpoch })),
  }));
}

export interface OffsetForLeaderEpochResponseShape {
  topics: readonly {
    topic: string;
    partitions: readonly { errorCode: number; partition: number }[];
  }[];
}

/**
 * Throw the first partition-level failure, scanning topics then partitions in wire order.
 * Attaches topic/partition extras the same way Produce does.
 */
export function checkOffsetForLeaderEpochErrors(data: OffsetForLeaderEpochResponseShape): void {
  for (const topic of data.topics) {
    const partitionWithError = topic.partitions.find((partition) => failure(partition.errorCode));
    if (partitionWithError) {
      throw createErrorFromCode(partitionWithError.errorCode, {
        topic: topic.topic,
        partition: partitionWithError.partition,
      });
    }
  }
}
