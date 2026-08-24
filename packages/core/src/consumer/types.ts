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
  /**
   * Caps how many records are delivered to the handler per internal processing cycle of an
   * already-fetched batch (Java's `max.poll.records`). This slices delivery client-side only:
   * it never shrinks the Fetch request (`maxBytes`/`maxBytesPerPartition` are unaffected), and
   * leftover records carry over to the next cycle instead of being dropped or re-fetched.
   * Defaults to `500` for `eachMessage`; `eachBatch` is unlimited unless set explicitly.
   */
  maxRecords?: number;
  eachBatch?: EachBatchHandler | null;
  eachMessage?: EachMessageHandler | null;
  signal?: AbortSignal;
}

export interface TopicPartitions {
  topic: string;
  partitions: number[];
}

export interface TopicPartition {
  topic: string;
  partition: number;
}

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

/**
 * Payload for {@link ConsumerHooks.onConsume}. Fires immediately before the user's handler runs
 * for this unit of work: once per message when running with `eachMessage`, or once per batch when
 * running with `eachBatch`. Exactly one of `message`/`batch` is set, matching the active mode.
 */
export interface OnConsumeEvent {
  topic: string;
  partition: number;
  /** Set in `eachMessage` mode. */
  message?: KafkaMessage;
  /** Set in `eachBatch` mode. */
  batch?: import('./batch').Batch;
}

export type OnConsumeHook = (event: OnConsumeEvent) => void | Promise<void>;

/**
 * Payload for {@link ConsumerHooks.onCommit}. Fires once per offset-commit attempt - auto-commit
 * or a manual {@link Consumer.commitOffsets} call - after the broker responds (or the attempt
 * fails). `error` is set on failure; `topics` is the set of topic/partition offsets committed (or
 * attempted).
 */
export interface OnCommitEvent {
  groupId: string;
  memberId: string;
  groupGenerationId: number;
  topics: TopicOffsets[];
  error?: unknown;
}

export type OnCommitHook = (event: OnCommitEvent) => void | Promise<void>;

/**
 * User-supplied hooks for a consumer's consume/commit paths. These are plain ordered async
 * callbacks, not an interceptor SPI: each array runs in registration order, one hook is always
 * awaited before the next starts, and a hook that throws is caught and logged - it never fails or
 * alters the underlying consume or commit outcome.
 */
export interface ConsumerHooks {
  /** Before `eachMessage`/`eachBatch` runs for a message or batch. */
  onConsume?: readonly OnConsumeHook[];
  /** After an offset-commit attempt (auto-commit or manual `commitOffsets`) settles. */
  onCommit?: readonly OnCommitHook[];
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
