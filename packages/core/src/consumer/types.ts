import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError } from '../errors';
import type { Logger } from '../loggers/index';
import type { RecordBatchContext, RecordHeaders } from '../protocol/records/record';
import type { RetryOptions } from '../retry/index';

/** Decoded record from a Fetch response. @see https://kafka.apache.org/43/implementation/messages/ */
export interface KafkaMessage {
  magicByte: number;
  attributes: number;
  timestamp: bigint;
  offset: bigint;
  key: Buffer | null;
  value: Buffer | null;
  headers: RecordHeaders;
  isControlRecord: boolean;
  batchContext: RecordBatchContext;
  /** This message's on-wire size (post-decompression); see `DecodedRecord.byteSize`. */
  byteSize: number;
}

export interface EachMessagePayload {
  topic: string;
  partition: number;
  message: KafkaMessage;
  heartbeat: () => Promise<void>;
  pause: () => () => void;
}

export interface EachBatchPayload {
  batch: import('./batch').Batch;
  resolveOffset: (offset: bigint) => void;
  heartbeat: () => Promise<void>;
  pause: () => () => void;
  commitOffsetsIfNecessary: (offsets?: Offsets) => Promise<void>;
  uncommittedOffsets: () => OffsetsByTopicPartition;
  isRunning: () => boolean;
  isStale: () => boolean;
}

export type EachBatchHandler = (payload: EachBatchPayload) => Promise<void>;
export type EachMessageHandler = (payload: EachMessagePayload) => Promise<void>;

/**
 * Options for {@link Consumer.run} and {@link Consumer.stream}.
 * @see https://kafka.apache.org/43/configuration/consumer-configs/
 */
export interface ConsumerRunConfig {
  /** Commit offsets automatically after processing. */
  autoCommit?: boolean;
  /**
   * Commit at least this often, in milliseconds. When both this and
   * `autoCommitThreshold` are unset, commit after each processed batch.
   */
  autoCommitInterval?: number | null;
  /** Commit after this many messages have been processed. */
  autoCommitThreshold?: number | null;
  /** When true, resolve the last offset of each batch after `eachBatch` returns. */
  eachBatchAutoResolve?: boolean;
  /** How many partitions to process in parallel. */
  partitionsConsumedConcurrently?: number;
  /** Max queued+in-flight batches prefetched per broker node. */
  prefetchMaxBatches?: number;
  /** Max queued+in-flight record bytes prefetched per broker node. */
  prefetchMaxBytes?: number;
  eachBatch?: EachBatchHandler | null;
  eachMessage?: EachMessageHandler | null;
  signal?: AbortSignal;
  /**
   * Called with the partitions this member is giving up, before the consumer fetches from its
   * new assignment (revoke-before-reassign). For a classic (eager) rebalance this is the
   * member's entire prior assignment; for a cooperative-sticky incremental rebalance it is only
   * the subset actually being given up this round - partitions the member keeps across the
   * rebalance are not reported. Not called when partitions are lost without a clean revoke, see
   * {@link ConsumerRunConfig.onPartitionsLost}. Awaited before the next assignment is installed;
   * an error thrown here is logged and does not abort the rebalance.
   */
  onPartitionsRevoked?: RebalanceListener;
  /**
   * Called with the partitions newly gained by this member once it has (re)joined the group and
   * installed its new assignment. For a classic rebalance this is the member's entire new
   * assignment; for a cooperative-sticky incremental rebalance it is only the partitions not
   * already held before this rebalance. An error thrown here is logged and does not abort the
   * rebalance.
   */
  onPartitionsAssigned?: RebalanceListener;
  /**
   * Called instead of `onPartitionsRevoked` when this member's assignment was lost without a
   * clean revoke - e.g. its session expired, or it was fenced out of the group
   * (`UNKNOWN_MEMBER_ID` / `FENCED_MEMBER_EPOCH`) before it had a chance to leave gracefully. A
   * lost partition may already be owned by another member, so any pending offset commit for it
   * should typically be abandoned rather than attempted. An error thrown here is logged and does
   * not abort the rejoin.
   */
  onPartitionsLost?: RebalanceListener;
}

export interface TopicPartitions {
  topic: string;
  partitions: number[];
}

export interface TopicPartition {
  topic: string;
  partition: number;
}

/**
 * Rebalance callback for {@link ConsumerRunConfig.onPartitionsRevoked},
 * {@link ConsumerRunConfig.onPartitionsAssigned}, and {@link ConsumerRunConfig.onPartitionsLost}.
 * Called with only the partitions actually moving in that rebalance step (see each option's doc
 * comment). Ecosystem term for the equivalent protocol concept: Java's `ConsumerRebalanceListener`.
 * @see https://kafka.apache.org/43/design/design/
 */
export type RebalanceListener = (topicPartitions: TopicPartition[]) => void | Promise<void>;

export interface TopicPartitionOffset extends TopicPartition {
  offset: bigint;
}

export interface TopicPartitionOffsetAndMetadata extends TopicPartitionOffset {
  metadata?: string | null;
}

export interface PartitionOffset {
  partition: number;
  offset: bigint;
}

export interface TopicOffsets {
  topic: string;
  partitions: PartitionOffset[];
}

export interface Offsets {
  topics: TopicOffsets[];
}

export interface OffsetsByTopicPartition {
  topics: TopicOffsets[];
}

export type MemberAssignment = Record<string, number[]>;

export interface GroupMember {
  memberId: string;
  memberMetadata: Buffer;
  groupInstanceId?: string | null;
}

export interface GroupMemberAssignment {
  memberId: string;
  memberAssignment: Buffer;
}

export interface GroupProtocol {
  name: string;
  metadata: Buffer;
}

/**
 * Partition assigner used during SyncGroup.
 * @see https://kafka.apache.org/43/design/design/
 */
export interface Assigner {
  name: string;
  version: number;
  /** Classic eager (revoke all) vs incremental cooperative (KIP-429). */
  readonly protocolType?: 'eager' | 'cooperative';
  assign: (group: { members: readonly GroupMember[]; topics: readonly string[] }) => Promise<GroupMemberAssignment[]>;
  protocol: (subscription: { topics: readonly string[] }) => GroupProtocol;
  /** Persist this member's current assignment so the next JoinGroup metadata stays sticky. */
  onAssignment?: (assignment: MemberAssignment) => void;
}

export type PartitionAssigner = (config: { cluster: Cluster; groupId: string; logger: Logger }) => Assigner;

export type ConsumerRetryOptions = RetryOptions & {
  restartOnFailure?: (error: Error) => Promise<boolean>;
};

export interface MemberDescription {
  clientHost: string;
  clientId: string;
  memberId: string;
  memberAssignment: Buffer;
  memberMetadata: Buffer;
}

export type ConsumerGroupState = 'Unknown' | 'PreparingRebalance' | 'CompletingRebalance' | 'Stable' | 'Dead' | 'Empty';

export interface GroupDescription {
  groupId: string;
  members: MemberDescription[];
  protocol: string;
  protocolType: string;
  state: string;
}

/** Coerce a user-supplied offset to `bigint`. Accepts `bigint`, integer `number`, and numeric strings. */
export function parseOffset(offset: unknown): bigint {
  if (typeof offset === 'bigint') return offset;
  if (typeof offset === 'number' && Number.isInteger(offset)) return BigInt(offset);
  if (typeof offset === 'string' && offset !== '') {
    try {
      return BigInt(offset);
    } catch {
      // fall through to the shared error
    }
  }

  throw new KafkaNonRetriableError(`Invalid offset, expected a long received ${String(offset)}`);
}
