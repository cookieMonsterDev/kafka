import type { Broker } from '../../broker/index';
import { KafkaNonRetriableError } from '../../errors';
import { CONSUMER_GROUP_LEAVE_EPOCH } from '../../protocol/requests/consumer-group-heartbeat/index';
import type { ConsumerGroupHeartbeatTopicPartitions } from '../../protocol/requests/consumer-group-heartbeat/index';
import type { ConsumerGroupHeartbeatResponseV1Body } from '../../protocol/requests/consumer-group-heartbeat/v1/response';
import { HEARTBEAT } from '../instrumentation-events';
import type { TopicOffsetConfiguration } from '../offset-reset';
import type { TopicPartitions } from '../types';
import { partitionsNotIn, type GroupMembership, type MembershipHost } from './group-membership';
import { HeartbeatMember } from './heartbeat-member';
import { TopicIdNames, type DescribedTopic } from './topic-id-names';

/** Upper bound on the back-to-back heartbeats one join or one assignment acknowledgement sends. */
const MAX_IMMEDIATE_HEARTBEATS = 8;

export interface ConsumerProtocolMembershipOptions {
  rebalanceTimeout: number;
  groupInstanceId: string | null;
  rackId: string;
  /** The broker-side assignor to request (`serverAssignor`), or `null` for the broker default. */
  serverAssignor: 'uniform' | 'range' | null;
  subscribedTopicRegex: string | null;
  /** Offset-reset configuration for a topic assigned only through `subscribedTopicRegex`. */
  defaultTopicConfiguration: TopicOffsetConfiguration | null;
  /** Heartbeat interval until the broker sends its own. */
  heartbeatInterval: number;
}

/** The consumer group protocol (KIP-848): ConsumerGroupHeartbeat with broker-side assignment. */
export class ConsumerProtocolMembership implements GroupMembership {
  readonly isGroupMember = true;
  readonly leaderId = null;
  readonly groupProtocol = 'consumer';
  coordinator: Broker | null = null;
  nextHeartbeatAt = Infinity;

  readonly #host: MembershipHost;
  readonly #options: ConsumerProtocolMembershipOptions;
  readonly #member = new HeartbeatMember();
  readonly #topicNames: TopicIdNames;
  #ownedTopicPartitions: ConsumerGroupHeartbeatTopicPartitions[] = [];
  /** The owned partitions changed since the last heartbeat, which has to report them. */
  #hasUnreportedPartitions = false;

  constructor(host: MembershipHost, options: ConsumerProtocolMembershipOptions) {
    this.#host = host;
    this.#options = options;
    this.#topicNames = new TopicIdNames({
      subscribedTopics: () => this.#host.topicsSubscribed,
      hasPattern: options.subscribedTopicRegex != null,
      describeAssignedTopics: () => this.#describeAssignedTopics(),
    });
  }

  get memberId(): string | null {
    return this.#member.memberId;
  }

  get generationId(): number {
    return this.#member.memberEpoch;
  }

  async join(): Promise<void> {
    const { cluster, groupId } = this.#host;
    if (!groupId) {
      throw new KafkaNonRetriableError('Consumer groupId is required to join a consumer group');
    }
    this.coordinator = await cluster.findGroupCoordinator({ groupId });
    this.#member.startJoin();
    this.#hasUnreportedPartitions = false;
    this.#ownedTopicPartitions = [];
    await cluster.refreshMetadata();

    await this.#heartbeat();

    let attempts = 0;
    while (!this.#member.isJoined && !this.#host.shuttingDown && attempts < MAX_IMMEDIATE_HEARTBEATS) {
      attempts += 1;
      await this.#heartbeat();
    }

    attempts = 0;
    while (this.#hasUnreportedPartitions && !this.#host.shuttingDown && attempts < MAX_IMMEDIATE_HEARTBEATS) {
      attempts += 1;
      await this.#heartbeat();
    }
  }

  async heartbeat(): Promise<void> {
    if (Date.now() < this.nextHeartbeatAt) return;
    await this.#heartbeat();
  }

  async leave(): Promise<void> {
    const { groupId } = this.#host;
    const { coordinator, memberId } = this;
    if (!memberId || !coordinator || !groupId) return;

    if (this.#member.isJoined) {
      await coordinator.consumerGroupHeartbeat({
        groupId,
        memberId,
        memberEpoch: CONSUMER_GROUP_LEAVE_EPOCH,
        instanceId: this.#options.groupInstanceId,
      });
    }
    this.#member.reset();
    this.#ownedTopicPartitions = [];
    this.#hasUnreportedPartitions = false;
    this.nextHeartbeatAt = Infinity;
  }

  resetRejectedMember({ forgetMemberId }: { forgetMemberId: boolean }): void {
    // A member with an unknown id joins as a new member; a fenced one rejoins under its own id.
    this.#member.reset({ forgetMemberId });
  }

  async #heartbeat(ackDepth = 0): Promise<void> {
    const { groupId, instrumentationEmitter } = this.#host;
    const { coordinator } = this;
    if (!coordinator || !groupId) {
      throw new KafkaNonRetriableError('Consumer group has not joined');
    }

    const member = this.#member;
    const memberId = member.ensureMemberId();
    const wasJoined = member.isJoined;
    // Epoch 0 is a (re)join. The broker returns INVALID_REQUEST unless rebalanceTimeoutMs is
    // set, subscribedTopicNames (or regex) is non-null, and TopicPartitions is an empty list —
    // null means "unchanged", which is illegal before the member exists.
    const isJoining = member.isJoining;
    const sendsJoinFields = member.sendsJoinFields;
    const { groupInstanceId, rackId } = this.#options;
    const response = await coordinator.consumerGroupHeartbeat({
      groupId,
      memberId,
      memberEpoch: member.memberEpoch,
      instanceId: sendsJoinFields ? groupInstanceId : null,
      rackId: sendsJoinFields && rackId !== '' ? rackId : null,
      rebalanceTimeoutMs: sendsJoinFields ? this.#options.rebalanceTimeout : -1,
      subscribedTopicNames: sendsJoinFields ? [...this.#host.topicsSubscribed] : null,
      subscribedTopicRegex: sendsJoinFields ? this.#options.subscribedTopicRegex : null,
      serverAssignor: sendsJoinFields ? this.#options.serverAssignor : null,
      topicPartitions: this.#reportedPartitions(isJoining),
    });

    this.#hasUnreportedPartitions = false;
    member.applyResponse(response);
    this.nextHeartbeatAt = member.nextHeartbeatAt(this.#options.heartbeatInterval);
    instrumentationEmitter.emit(HEARTBEAT, {
      groupId,
      memberId,
      groupGenerationId: response.memberEpoch,
      groupInstanceId,
    });

    await this.#applyHeartbeatResponse(response, wasJoined);

    // Ack the new assignment on the next heartbeat immediately. The broker will not give
    // revoked partitions to other members until this member reports the updated owned set.
    if (this.#hasUnreportedPartitions && ackDepth < MAX_IMMEDIATE_HEARTBEATS) {
      await this.#heartbeat(ackDepth + 1);
    }
  }

  /** An empty list on a join, the owned partitions after they changed, `null` ("unchanged") otherwise. */
  #reportedPartitions(isJoining: boolean): ConsumerGroupHeartbeatTopicPartitions[] | null {
    if (isJoining) return [];
    return this.#hasUnreportedPartitions ? this.#ownedTopicPartitions : null;
  }

  async #applyHeartbeatResponse(response: ConsumerGroupHeartbeatResponseV1Body, wasJoined: boolean): Promise<void> {
    const host = this.#host;
    // The epoch can advance without a new assignment (so no new OffsetManager); commits must carry
    // the current epoch or the broker rejects them with STALE_MEMBER_EPOCH.
    host.updateMemberEpoch(response.memberEpoch);

    if (!response.assignment) return;

    const assignment: TopicPartitions[] = [];
    const owned: ConsumerGroupHeartbeatTopicPartitions[] = [];
    const { defaultTopicConfiguration } = this.#options;
    for (const { topicId, partitions } of response.assignment.topicPartitions) {
      const topic = await this.#topicNames.resolve(topicId);
      // Topics assigned only through `subscribedTopicRegex` were unknown at subscribe time, so
      // they have no entry in `topicConfigurations` by name yet - apply the regex subscription's
      // offset-reset config the first time each such topic is seen.
      if (defaultTopicConfiguration && !(topic in host.topicConfigurations)) {
        host.topicConfigurations[topic] = defaultTopicConfiguration;
      }
      assignment.push({ topic, partitions });
      owned.push({ topicId, partitions });
    }

    host.logger.debug('Received consumer-protocol assignment', {
      groupId: host.groupId,
      memberId: this.memberId,
      memberEpoch: this.generationId,
      memberAssignment: assignment,
    });

    // KIP-848 reconciliation is incremental at the wire level - the broker sends this member's
    // full current target assignment on every change, and the client diffs it against what it
    // already held, like a cooperative rebalance.
    const previousAssignment = host.assigned();
    await host.changeAssignment({
      assignment,
      revoked: partitionsNotIn(previousAssignment, assignment),
      gained: partitionsNotIn(assignment, previousAssignment),
      onInstalled: () => {
        this.#ownedTopicPartitions = owned;
        this.#hasUnreportedPartitions = true;
      },
    });

    // A reassignment while already in the group took no join, so it reports a zero duration.
    if (wasJoined) host.emitGroupJoin(0);
  }

  async #describeAssignedTopics(): Promise<DescribedTopic[]> {
    const { groupId } = this.#host;
    const { coordinator, memberId } = this;
    if (!coordinator || !groupId) return [];

    const { groups } = await coordinator.consumerGroupDescribe({
      groupIds: [groupId],
      includeAuthorizedOperations: false,
    });
    const group = groups.find((entry) => entry.groupId === groupId);
    if (!group) return [];

    const members = memberId ? group.members.filter((entry) => entry.memberId === memberId) : group.members;
    return members.flatMap((entry) => [...entry.assignment.topicPartitions, ...entry.targetAssignment.topicPartitions]);
  }
}
