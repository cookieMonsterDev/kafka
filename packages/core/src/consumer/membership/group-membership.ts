import type { Broker } from '../../broker/index';
import type { Cluster } from '../../cluster/index';
import type { InstrumentationEventEmitter } from '../../instrumentation/emitter';
import type { Logger } from '../../loggers/index';
import type { TopicOffsetConfiguration } from '../offset-reset';
import type { Assigner, TopicPartitions } from '../types';

/**
 * How a consumer takes part in its group: the classic protocol, the consumer protocol (KIP-848),
 * or assign mode, which has no group at all. A consumer has one for its lifetime.
 */
export interface GroupMembership {
  /** `false` in assign mode, which consumes chosen partitions without joining a group. */
  readonly isGroupMember: boolean;
  readonly coordinator: Broker | null;
  readonly memberId: string | null;
  /** The generation (classic protocol) or member epoch (consumer protocol) commits are tagged with. */
  readonly generationId: number | null;
  readonly leaderId: string | null;
  /** The assigner the classic protocol chose, or `'consumer'` for the consumer protocol. */
  readonly groupProtocol: string | null;
  /**
   * When the next heartbeat is due, in epoch milliseconds, or `Infinity` when none is. A plain
   * number so the runner can compare it per record without a call.
   */
  readonly nextHeartbeatAt: number;
  /** Joins the group and returns once the member's assignment is installed and stable. */
  join(): Promise<void>;
  /** Sends a heartbeat when one is due, applying any assignment change it returns. */
  heartbeat(): Promise<void>;
  leave(): Promise<void>;
  /**
   * Forgets the member state the coordinator rejected with `UNKNOWN_MEMBER_ID` or
   * `FENCED_MEMBER_EPOCH`, so the next {@link join} starts over.
   */
  resetFencedMember(error: { type?: string }): void;
}

/** One change of the member's assignment, with the partitions it revokes and gains. */
export interface AssignmentChange {
  /** The member's whole assignment after the change. */
  assignment: TopicPartitions[];
  revoked: readonly TopicPartitions[];
  gained: readonly TopicPartitions[];
  /** Runs once the assignment is installed, before `onPartitionsAssigned` is called. */
  onInstalled?: () => void;
}

/** The consumer group state and callbacks a {@link GroupMembership} works with. */
export interface MembershipHost {
  readonly cluster: Cluster;
  readonly groupId: string | null;
  readonly logger: Logger;
  readonly instrumentationEmitter: InstrumentationEventEmitter;
  readonly assigners: readonly Assigner[];
  readonly topicsSubscribed: readonly string[];
  readonly topicConfigurations: Record<string, TopicOffsetConfiguration>;
  readonly shuttingDown: boolean;
  assigned(): TopicPartitions[];
  /**
   * Calls `onPartitionsRevoked` with the revoked partitions, installs the new assignment, then
   * calls `onPartitionsAssigned` with the gained ones.
   */
  changeAssignment(change: AssignmentChange): Promise<void>;
  /** Tags later commits with `generationId`, which can change without a new assignment. */
  updateCommitGeneration(generationId: number): void;
  emitGroupJoin(durationMs: number): void;
}

/** The partitions of `assignment` that `other` does not contain. */
export function partitionsNotIn(
  assignment: readonly TopicPartitions[],
  other: readonly TopicPartitions[],
): TopicPartitions[] {
  const otherPartitionsByTopic = new Map(other.map(({ topic, partitions }) => [topic, new Set(partitions)]));

  return assignment
    .map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.filter((partition) => !otherPartitionsByTopic.get(topic)?.has(partition)),
    }))
    .filter(({ partitions }) => partitions.length > 0);
}

/** `UNKNOWN_MEMBER_ID` / `FENCED_MEMBER_EPOCH`: the coordinator no longer accepts this member. */
export function isFencedMemberError(error: { type?: string }): boolean {
  return error.type === 'UNKNOWN_MEMBER_ID' || error.type === 'FENCED_MEMBER_EPOCH';
}
