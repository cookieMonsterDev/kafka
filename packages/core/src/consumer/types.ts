import type { Cluster } from '../cluster/index.js';
import { KafkaJSNonRetriableError } from '../errors.js';
import type { Logger } from '../loggers/index.js';
import type { RecordBatchContext, RecordHeaders } from '../protocol/records/record.js';
import type { RetryOptions } from '../retry/index.js';

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
}

export interface EachMessagePayload {
  topic: string;
  partition: number;
  message: KafkaMessage;
  heartbeat: () => Promise<void>;
  pause: () => () => void;
}

export interface EachBatchPayload {
  batch: import('./batch.js').Batch;
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

export interface ConsumerRunConfig {
  autoCommit?: boolean;
  autoCommitInterval?: number | null;
  autoCommitThreshold?: number | null;
  eachBatchAutoResolve?: boolean;
  partitionsConsumedConcurrently?: number;
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

export interface Assigner {
  name: string;
  version: number;
  assign: (group: { members: readonly GroupMember[]; topics: readonly string[] }) => Promise<GroupMemberAssignment[]>;
  protocol: (subscription: { topics: readonly string[] }) => GroupProtocol;
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
 * Coerce a user-supplied offset to `bigint`. Accepts `bigint` and integer `number` at the type
 * level; strings are still parsed at runtime so plain-JS callers that haven't migrated off
 * kafkajs's string offsets don't fail on a type-system-only change.
 */
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

  throw new KafkaJSNonRetriableError(`Invalid offset, expected a long received ${String(offset)}`);
}
