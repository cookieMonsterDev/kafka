import type { Broker } from '../broker/index';
import type { Cluster } from '../cluster/index';
import type { TopicPartitions } from '../consumer/types';
import { HeartbeatMember } from '../consumer/membership/heartbeat-member';
import { TopicIdNames, type DescribedTopic } from '../consumer/membership/topic-id-names';
import { KafkaError, KafkaNonRetriableError } from '../errors';
import type { Logger } from '../loggers/index';
import { SHARE_GROUP_LEAVE_EPOCH } from '../protocol/requests/share-group-heartbeat/index';
import type { ShareGroupHeartbeatResponseV1Body } from '../protocol/requests/share-group-heartbeat/v1/response';
import { retrier, type RetryOptions } from '../retry/index';
import { sleep } from '../utils/wait';

const JOIN_DEADLINE_MS = 30_000;

export interface ShareGroupOptions {
  cluster: Cluster;
  groupId: string;
  logger: Logger;
  retry?: RetryOptions;
  rackId?: string;
}

/** Share-group membership via ShareGroupHeartbeat (KIP-932). */
export class ShareGroup {
  readonly groupId: string;
  readonly cluster: Cluster;
  readonly #logger: Logger;
  readonly rackId: string;

  /** Member id, epoch and heartbeat state, shared with the consumer group protocol (KIP-848). */
  readonly member = new HeartbeatMember();
  coordinator: Broker | null = null;
  topicsSubscribed: string[] = [];
  assignment: TopicPartitions[] = [];
  shuttingDown = false;
  readonly #topicNames: TopicIdNames;
  readonly #retrier: ReturnType<typeof retrier>;

  constructor({ cluster, groupId, logger, retry, rackId = '' }: ShareGroupOptions) {
    this.cluster = cluster;
    this.groupId = groupId;
    this.#logger = logger.namespace('ShareGroup');
    this.#retrier = retrier(retry);
    this.rackId = rackId;
    this.#topicNames = new TopicIdNames({
      subscribedTopics: () => this.topicsSubscribed,
      hasPattern: false,
      describeAssignedTopics: () => this.#describeAssignedTopics(),
    });
  }

  get memberId(): string | null {
    return this.member.memberId;
  }

  get heartbeatIntervalMs(): number | null {
    return this.member.heartbeatIntervalMs;
  }

  async connect(): Promise<void> {
    await this.cluster.connect();
  }

  async disconnect(): Promise<void> {
    if (this.member.isJoined) await this.leave();
    await this.cluster.disconnect();
  }

  subscribe(topics: readonly string[]): void {
    this.topicsSubscribed = [...topics];
  }

  async joinAndSync(): Promise<void> {
    await this.#retrier(async () => {
      this.coordinator = await this.cluster.findGroupCoordinator({ groupId: this.groupId });
      this.member.startJoin();
      await this.cluster.refreshMetadata();
      await this.#heartbeat({ force: true });

      const deadline = Date.now() + JOIN_DEADLINE_MS;
      while (!this.member.isJoined && !this.shuttingDown && Date.now() < deadline) {
        const waitMs = this.heartbeatIntervalMs ?? 500;
        await sleep(Math.max(50, waitMs));
        await this.#heartbeat({ force: true });
      }

      if (!this.member.isJoined && !this.shuttingDown) {
        throw new KafkaError(`Share group join timed out for group ${this.groupId}`);
      }
    });
  }

  async leave(): Promise<void> {
    const { memberId } = this;
    if (!this.coordinator || !memberId) return;
    this.shuttingDown = true;
    try {
      await this.coordinator.shareGroupHeartbeat({
        groupId: this.groupId,
        memberId,
        memberEpoch: SHARE_GROUP_LEAVE_EPOCH,
        rackId: null,
        subscribedTopicNames: null,
      });
    } catch (error) {
      this.#logger.warn('Share group leave failed', {
        groupId: this.groupId,
        error: error instanceof Error ? error.message : String(error),
      });
    } finally {
      this.member.reset();
    }
  }

  async heartbeat({ interval = 0, force = false }: { interval?: number; force?: boolean } = {}): Promise<void> {
    await this.#heartbeat({ interval, force });
  }

  heartbeatDue(interval: number): boolean {
    if (!this.coordinator) return false;
    return Date.now() >= this.member.nextHeartbeatAt(interval);
  }

  async #heartbeat({ interval = 0, force = false }: { interval?: number; force?: boolean }): Promise<void> {
    if (this.shuttingDown || !this.coordinator) return;
    if (!force && Date.now() < this.member.nextHeartbeatAt(interval)) return;

    const memberId = this.member.ensureMemberId();
    const sendsJoinFields = this.member.sendsJoinFields;

    const response = await this.#retrier(() => {
      if (!this.coordinator) throw new KafkaNonRetriableError('Share group coordinator is not set');
      return this.coordinator.shareGroupHeartbeat({
        groupId: this.groupId,
        memberId,
        memberEpoch: this.member.memberEpoch,
        rackId: sendsJoinFields && this.rackId !== '' ? this.rackId : null,
        subscribedTopicNames: sendsJoinFields ? [...this.topicsSubscribed] : null,
      });
    });

    this.member.applyResponse(response);
    await this.#applyAssignment(response);
  }

  async #applyAssignment(response: ShareGroupHeartbeatResponseV1Body): Promise<void> {
    if (!response.assignment) return;

    const nextAssignment: TopicPartitions[] = [];
    for (const { topicId, partitions } of response.assignment.topicPartitions) {
      const topic = await this.#topicNames.resolve(topicId);
      nextAssignment.push({ topic, partitions });
    }
    this.assignment = nextAssignment;
    this.#logger.debug('Received share-group assignment', {
      groupId: this.groupId,
      memberId: this.memberId,
      memberEpoch: this.member.memberEpoch,
      assignment: this.assignment,
    });
  }

  async #describeAssignedTopics(): Promise<DescribedTopic[]> {
    const { coordinator, groupId } = this;
    if (!coordinator) return [];

    const { groups } = await coordinator.shareGroupDescribe({
      groupIds: [groupId],
      includeAuthorizedOperations: false,
    });
    const group = groups.find((entry) => entry.groupId === groupId);
    return group?.members.flatMap((entry) => entry.assignment.topicPartitions) ?? [];
  }

  assigned(): TopicPartitions[] {
    return this.assignment;
  }

  hasAssignment(topic: string, partition: number): boolean {
    return this.assignment.some((entry) => entry.topic === topic && entry.partitions.includes(partition));
  }

  getNodeIds(): string[] {
    const nodeIds = new Set<string>();
    for (const { topic, partitions } of this.assignment) {
      const leaders = this.cluster.findLeaderForPartitions(topic, partitions);
      for (const nodeId of Object.keys(leaders)) nodeIds.add(nodeId);
    }
    return [...nodeIds];
  }

  filterPartitionsByNode(nodeId: string, topicPartitions: readonly TopicPartitions[]): TopicPartitions[] {
    return topicPartitions
      .map(({ topic, partitions }) => {
        const leaders = this.cluster.findLeaderForPartitions(topic, partitions);
        return { topic, partitions: leaders[Number(nodeId)] ?? [] };
      })
      .filter(({ partitions }) => partitions.length > 0);
  }

  async recoverFromFetch(error: unknown): Promise<void> {
    const type = (error as { type?: string }).type;
    if (type === 'NOT_COORDINATOR_FOR_GROUP' || type === 'GROUP_COORDINATOR_NOT_AVAILABLE') {
      this.coordinator = await this.cluster.findGroupCoordinator({ groupId: this.groupId });
    }
  }
}
