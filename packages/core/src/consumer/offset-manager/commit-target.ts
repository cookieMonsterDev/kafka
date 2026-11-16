import type { Broker } from '../../broker/index';
import type { Cluster } from '../../cluster/index';
import { KafkaError, KafkaNonRetriableError } from '../../errors';
import type { GroupMembership } from '../membership/group-membership';

/** The member fields an OffsetCommit carries. */
export interface CommitIdentity {
  memberId: string;
  groupGenerationId: number;
}

/**
 * Where an offset tracker commits offsets and keeps the committed offsets it knows: the consumer
 * group as a member, or the group of an assign-mode consumer, which may have none.
 */
export interface CommitTarget {
  /** The group offsets are committed to and fetched from, or `null` when there is none. */
  readonly groupId: string | null;
  /**
   * `true` for a group member, whose seek is committed to the group (or left uncommitted without
   * auto-commit). An assign-mode seek only moves the local position.
   */
  readonly isGroupMember: boolean;
  /** Read when each OffsetCommit is sent, so it carries the member's current generation or member epoch. */
  commitIdentity(): CommitIdentity;
  coordinator(): Promise<Broker>;
  committedOffset(topic: string, partition: number): bigint | undefined;
  setCommittedOffset(topic: string, partition: number, offset: bigint): void;
  deleteCommittedOffset(topic: string, partition: number): void;
}

/**
 * A consumer group member. Its identity and coordinator come from the group membership, and its
 * committed offsets live in the cluster's per-group record, which a transactional producer of the
 * same client also writes when it sends offsets for the group.
 */
export class GroupMemberCommitTarget implements CommitTarget {
  readonly isGroupMember = true;
  readonly groupId: string;
  readonly #cluster: Cluster;
  readonly #membership: Pick<GroupMembership, 'coordinator' | 'memberId' | 'generationId'>;

  constructor(
    cluster: Cluster,
    groupId: string,
    membership: Pick<GroupMembership, 'coordinator' | 'memberId' | 'generationId'>,
  ) {
    this.#cluster = cluster;
    this.groupId = groupId;
    this.#membership = membership;
  }

  commitIdentity(): CommitIdentity {
    const { memberId, generationId } = this.#membership;
    if (memberId == null || generationId == null) {
      // Retriable: a rejoin in progress restores the identity.
      throw new KafkaError('Consumer group has not joined');
    }
    return { memberId, groupGenerationId: generationId };
  }

  async coordinator(): Promise<Broker> {
    const { coordinator } = this.#membership;
    if (!coordinator) {
      throw new KafkaError('Consumer group has not joined');
    }
    if (coordinator.isConnected()) return coordinator;
    return this.#cluster.findBroker({ nodeId: String(coordinator.nodeId) });
  }

  committedOffset(topic: string, partition: number): bigint | undefined {
    return this.#cluster.committedOffset({ groupId: this.groupId, topic, partition });
  }

  setCommittedOffset(topic: string, partition: number, offset: bigint): void {
    this.#cluster.markOffsetAsCommitted({ groupId: this.groupId, topic, partition, offset });
  }

  deleteCommittedOffset(topic: string, partition: number): void {
    this.#cluster.forgetCommittedOffset({ groupId: this.groupId, topic, partition });
  }
}

/**
 * `OffsetCommit` outside of any group membership uses these sentinels, Kafka's simple-consumer
 * convention: the broker skips generation and membership validation when `groupGenerationId` is
 * `-1` and `memberId` is empty.
 */
const ASSIGN_MODE_IDENTITY: CommitIdentity = Object.freeze({ memberId: '', groupGenerationId: -1 });

/**
 * An assign-mode consumer. With a group id it commits as a simple consumer (see
 * {@link ASSIGN_MODE_IDENTITY}); without one, offsets have nowhere to go. Its committed offsets
 * are kept locally, and a seek sets one directly as the position consumption starts from.
 */
export class AssignModeCommitTarget implements CommitTarget {
  readonly isGroupMember = false;
  readonly groupId: string | null;
  readonly #cluster: Cluster;
  readonly #committedOffsets = new Map<string, Map<number, bigint>>();
  #coordinator: Broker | null = null;

  constructor(cluster: Cluster, groupId: string | null) {
    this.#cluster = cluster;
    this.groupId = groupId;
  }

  commitIdentity(): CommitIdentity {
    return ASSIGN_MODE_IDENTITY;
  }

  async coordinator(): Promise<Broker> {
    if (!this.groupId) {
      throw new KafkaNonRetriableError(
        'Cannot reach a group coordinator in assign() mode without a configured groupId.',
      );
    }

    if (!this.#coordinator || !this.#coordinator.isConnected()) {
      this.#coordinator = await this.#cluster.findGroupCoordinator({ groupId: this.groupId });
    }
    return this.#coordinator;
  }

  committedOffset(topic: string, partition: number): bigint | undefined {
    return this.#committedOffsets.get(topic)?.get(partition);
  }

  setCommittedOffset(topic: string, partition: number, offset: bigint): void {
    let committedPartitions = this.#committedOffsets.get(topic);
    if (!committedPartitions) {
      committedPartitions = new Map();
      this.#committedOffsets.set(topic, committedPartitions);
    }
    committedPartitions.set(partition, offset);
  }

  deleteCommittedOffset(topic: string, partition: number): void {
    this.#committedOffsets.get(topic)?.delete(partition);
  }
}
