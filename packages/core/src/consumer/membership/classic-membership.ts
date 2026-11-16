import type { Broker } from '../../broker/index';
import { KafkaNonRetriableError } from '../../errors';
import { arrayDiff } from '../../utils/array-diff';
import { MemberAssignment } from '../assigner-protocol';
import { HEARTBEAT, RECEIVED_UNSUBSCRIBED_TOPICS } from '../instrumentation-events';
import type { MemberAssignment as MemberAssignmentMap, TopicPartitions } from '../types';
import { partitionsNotIn, type GroupMembership, type MembershipHost } from './group-membership';

export interface ClassicMembershipOptions {
  sessionTimeout: number;
  rebalanceTimeout: number;
  groupInstanceId: string | null;
  heartbeatInterval: number;
}

interface GroupMember {
  memberId: string;
  memberMetadata: Buffer;
  groupInstanceId?: string | null;
}

/** The classic group protocol: JoinGroup and SyncGroup, with the leader running a client-side assigner. */
export class ClassicMembership implements GroupMembership {
  readonly isGroupMember = true;
  coordinator: Broker | null = null;
  memberId: string | null = null;
  generationId: number | null = null;
  leaderId: string | null = null;
  groupProtocol: string | null = null;
  nextHeartbeatAt = Infinity;

  readonly #host: MembershipHost;
  readonly #options: ClassicMembershipOptions;
  #members: GroupMember[] = [];
  #lastHeartbeatAt = Date.now();

  constructor(host: MembershipHost, options: ClassicMembershipOptions) {
    this.#host = host;
    this.#options = options;
  }

  async join(): Promise<void> {
    let requiresFollowupRebalance: boolean;
    do {
      await this.#joinGroup();
      if (this.#host.shuttingDown) return;
      requiresFollowupRebalance = await this.#syncGroup();
    } while (requiresFollowupRebalance && !this.#host.shuttingDown);
  }

  async heartbeat(): Promise<void> {
    const { groupId } = this.#host;
    const { coordinator, memberId, generationId } = this;
    if (!groupId || !coordinator || memberId == null || generationId == null) return;
    if (Date.now() < this.nextHeartbeatAt) return;

    const payload = {
      groupId,
      memberId,
      groupGenerationId: generationId,
      groupInstanceId: this.#options.groupInstanceId,
    };
    await coordinator.heartbeat(payload);
    this.#host.instrumentationEmitter.emit(HEARTBEAT, payload);
    this.#lastHeartbeatAt = Date.now();
    this.#updateNextHeartbeatAt();
  }

  async leave(): Promise<void> {
    const { groupId } = this.#host;
    const { memberId, coordinator } = this;
    if (!memberId || !coordinator || !groupId) return;

    await coordinator.leaveGroup({ groupId, memberId, groupInstanceId: this.#options.groupInstanceId });
    this.memberId = null;
    this.#updateNextHeartbeatAt();
  }

  resetFencedMember(error: { type?: string }): void {
    if (error.type === 'UNKNOWN_MEMBER_ID') this.memberId = null;
    this.#updateNextHeartbeatAt();
  }

  #isLeader(): boolean {
    return this.leaderId != null && this.memberId === this.leaderId;
  }

  #updateNextHeartbeatAt(): void {
    const isMember = this.coordinator != null && this.memberId != null && this.generationId != null;
    this.nextHeartbeatAt = isMember ? this.#lastHeartbeatAt + this.#options.heartbeatInterval : Infinity;
  }

  async #joinGroup(): Promise<void> {
    const { cluster, groupId, assigners, topicsSubscribed } = this.#host;
    if (!groupId) {
      throw new KafkaNonRetriableError('Consumer groupId is required to join a consumer group');
    }
    this.coordinator = await cluster.findGroupCoordinator({ groupId });

    const groupData = await this.coordinator.joinGroup({
      groupId,
      sessionTimeout: this.#options.sessionTimeout,
      rebalanceTimeout: this.#options.rebalanceTimeout,
      memberId: this.memberId ?? '',
      groupInstanceId: this.#options.groupInstanceId,
      protocolType: 'consumer',
      groupProtocols: assigners.map((assigner) => assigner.protocol({ topics: [...topicsSubscribed] })),
    });

    this.generationId = groupData.generationId;
    this.leaderId = groupData.leaderId;
    this.memberId = groupData.memberId;
    this.#members = groupData.members;
    this.groupProtocol = groupData.groupProtocol;
    this.#updateNextHeartbeatAt();
  }

  /** Returns whether a cooperative assigner revoked partitions, which takes another rebalance to settle. */
  async #syncGroup(): Promise<boolean> {
    const host = this.#host;
    const { groupId, assigners, logger } = host;
    const topicsSubscribed = [...host.topicsSubscribed];
    const { generationId, memberId, groupProtocol, coordinator } = this;
    let assignment: { memberId: string; memberAssignment: Buffer }[] = [];

    if (!coordinator || !groupId || generationId == null || memberId == null) {
      throw new KafkaNonRetriableError('Consumer group has not joined');
    }

    const selectedAssigner = assigners.find(({ name }) => name === groupProtocol);
    if (this.#isLeader()) {
      logger.debug('Chosen as group leader', { groupId, generationId, memberId, topics: topicsSubscribed });

      if (!selectedAssigner) {
        throw new KafkaNonRetriableError(
          `Unsupported partition assigner "${groupProtocol}", the assigner wasn't found in the assigners list`,
        );
      }

      await host.cluster.refreshMetadata();
      assignment = await selectedAssigner.assign({ members: this.#members, topics: topicsSubscribed });

      logger.debug('Group assignment', { groupId, generationId, groupProtocol, assignment, topics: topicsSubscribed });
    }

    const { memberAssignment } = await coordinator.syncGroup({
      groupId,
      generationId,
      memberId,
      groupInstanceId: this.#options.groupInstanceId,
      protocolType: 'consumer',
      protocolName: groupProtocol,
      groupAssignment: assignment,
    });

    const decodedAssignment = MemberAssignment.decode(memberAssignment)?.assignment ?? {};
    logger.debug('Received assignment', { groupId, generationId, memberId, memberAssignment: decodedAssignment });

    const assignedTopics = Object.keys(decodedAssignment);
    const topicsNotSubscribed = arrayDiff(assignedTopics, topicsSubscribed);

    if (topicsNotSubscribed.length > 0) {
      const payload = { groupId, generationId, memberId, assignedTopics, topicsSubscribed, topicsNotSubscribed };
      host.instrumentationEmitter.emit(RECEIVED_UNSUBSCRIBED_TOPICS, payload);
      logger.warn('Consumer group received unsubscribed topics', payload);
    }

    const nextAssignment: TopicPartitions[] = arrayDiff(assignedTopics, topicsNotSubscribed).map((topic) => ({
      topic,
      partitions: decodedAssignment[topic] ?? [],
    }));

    const previousAssignment = host.assigned();
    const isCooperative = selectedAssigner?.protocolType === 'cooperative';
    // Cooperative-sticky (KIP-429) settles over two JoinGroup/SyncGroup rounds: this round's
    // assignment already leaves foreign-owned partitions unassigned (see
    // `applyCooperativeConstraint` in sticky-assigner.ts), so `partitionsToRevoke` is only the
    // subset this member is actually giving up - not its whole previous assignment. A classic
    // (eager) rebalance has no such settling round: the member's entire prior assignment is
    // revoked and its entire new assignment is granted in one round.
    const partitionsToRevoke = isCooperative ? partitionsNotIn(previousAssignment, nextAssignment) : [];

    await host.changeAssignment({
      assignment: nextAssignment,
      revoked: isCooperative ? partitionsToRevoke : previousAssignment,
      gained: isCooperative ? partitionsNotIn(nextAssignment, previousAssignment) : nextAssignment,
      onInstalled: () => {
        const partitionsByTopic: MemberAssignmentMap = Object.create(null) as MemberAssignmentMap;
        for (const { topic, partitions } of nextAssignment) partitionsByTopic[topic] = partitions;
        selectedAssigner?.onAssignment?.(partitionsByTopic);
      },
    });

    if (partitionsToRevoke.length > 0) {
      logger.debug('Cooperative assignment revoked partitions; rejoining to settle assignment', {
        groupId,
        generationId,
        memberId,
        partitionsToRevoke,
      });
    }

    return partitionsToRevoke.length > 0;
  }
}
