import { randomUUID } from 'node:crypto';
import type { Broker } from '../broker/index';
import type { Cluster } from '../cluster/index';
import type { TopicPartitions } from '../consumer/types';
import { KafkaError, KafkaNonRetriableError } from '../errors';
import type { Logger } from '../loggers/index';
import { SHARE_GROUP_JOIN_EPOCH, SHARE_GROUP_LEAVE_EPOCH } from '../protocol/requests/share-group-heartbeat/index';
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

  memberId: string | null = null;
  memberEpoch = SHARE_GROUP_JOIN_EPOCH;
  heartbeatIntervalMs: number | null = null;
  coordinator: Broker | null = null;
  topicsSubscribed: string[] = [];
  assignment: TopicPartitions[] = [];
  joined = false;
  shuttingDown = false;
  lastHeartbeatAt = 0;
  #includeJoinFields = false;
  #topicNameById = new Map<string, string>();
  readonly #retrier: ReturnType<typeof retrier>;

  constructor({ cluster, groupId, logger, retry, rackId = '' }: ShareGroupOptions) {
    this.cluster = cluster;
    this.groupId = groupId;
    this.#logger = logger.namespace('ShareGroup');
    this.#retrier = retrier(retry);
    this.rackId = rackId;
  }

  async connect(): Promise<void> {
    await this.cluster.connect();
  }

  async disconnect(): Promise<void> {
    if (this.joined) await this.leave();
    await this.cluster.disconnect();
  }

  subscribe(topics: readonly string[]): void {
    this.topicsSubscribed = [...topics];
  }

  async joinAndSync(): Promise<void> {
    await this.#retrier(async () => {
      this.coordinator = await this.cluster.findGroupCoordinator({ groupId: this.groupId });
      this.#includeJoinFields = true;
      this.memberEpoch = SHARE_GROUP_JOIN_EPOCH;
      this.joined = false;
      this.#ensureMemberId();
      await this.cluster.refreshMetadata();
      await this.#heartbeat({ force: true });

      const deadline = Date.now() + JOIN_DEADLINE_MS;
      while (!this.joined && !this.shuttingDown && Date.now() < deadline) {
        const waitMs = this.heartbeatIntervalMs ?? 500;
        await sleep(Math.max(50, waitMs));
        await this.#heartbeat({ force: true });
      }

      if (!this.joined && !this.shuttingDown) {
        throw new KafkaError(`Share group join timed out for group ${this.groupId}`);
      }
    });
  }

  async leave(): Promise<void> {
    if (!this.coordinator || !this.memberId) return;
    this.shuttingDown = true;
    try {
      await this.coordinator.shareGroupHeartbeat({
        groupId: this.groupId,
        memberId: this.memberId,
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
      this.joined = false;
      this.memberEpoch = SHARE_GROUP_JOIN_EPOCH;
    }
  }

  async heartbeat({ interval = 0, force = false }: { interval?: number; force?: boolean } = {}): Promise<void> {
    await this.#heartbeat({ interval, force });
  }

  heartbeatDue(interval: number): boolean {
    if (!this.coordinator) return false;
    const heartbeatInterval = this.heartbeatIntervalMs ?? interval;
    return Date.now() >= this.lastHeartbeatAt + heartbeatInterval;
  }

  async #heartbeat({ interval = 0, force = false }: { interval?: number; force?: boolean }): Promise<void> {
    if (this.shuttingDown || !this.coordinator) return;

    const heartbeatInterval = this.heartbeatIntervalMs ?? interval;
    const now = Date.now();
    if (!force && heartbeatInterval > 0 && now < this.lastHeartbeatAt + heartbeatInterval) return;

    const memberId = this.#ensureMemberId();
    const isJoining = this.memberEpoch === SHARE_GROUP_JOIN_EPOCH;
    const includeJoinFields = this.#includeJoinFields || isJoining;

    const response = await this.#retrier(() => {
      if (!this.coordinator) throw new KafkaNonRetriableError('Share group coordinator is not set');
      return this.coordinator.shareGroupHeartbeat({
        groupId: this.groupId,
        memberId,
        memberEpoch: this.memberEpoch,
        rackId: includeJoinFields ? (this.rackId === '' ? null : this.rackId) : null,
        subscribedTopicNames: includeJoinFields ? [...this.topicsSubscribed] : null,
      });
    });

    this.#includeJoinFields = false;
    this.lastHeartbeatAt = Date.now();
    await this.#applyHeartbeatResponse(response);
  }

  async #applyHeartbeatResponse(response: ShareGroupHeartbeatResponseV1Body): Promise<void> {
    if (response.memberId) this.memberId = response.memberId;
    this.memberEpoch = response.memberEpoch;
    this.heartbeatIntervalMs = response.heartbeatIntervalMs;
    if (response.memberEpoch > SHARE_GROUP_JOIN_EPOCH) this.joined = true;

    if (!response.assignment) return;

    const nextAssignment: TopicPartitions[] = [];
    for (const { topicId, partitions } of response.assignment.topicPartitions) {
      const topic = await this.#resolveTopicName(topicId);
      nextAssignment.push({ topic, partitions });
    }
    this.assignment = nextAssignment;
    this.#logger.debug('Received share-group assignment', {
      groupId: this.groupId,
      memberId: this.memberId,
      memberEpoch: this.memberEpoch,
      assignment: this.assignment,
    });
  }

  #topicIdKey(topicId: Buffer): string {
    return topicId.toString('hex');
  }

  async #resolveTopicName(topicId: Buffer): Promise<string> {
    const key = this.#topicIdKey(topicId);
    const cached = this.#topicNameById.get(key);
    if (cached) return cached;

    if (this.topicsSubscribed.length === 1) {
      const name = this.topicsSubscribed[0];
      if (name != null) {
        this.#topicNameById.set(key, name);
        return name;
      }
    }

    await this.#refreshTopicIdMapFromDescribe();
    const resolved = this.#topicNameById.get(key);
    if (resolved) return resolved;

    throw new KafkaNonRetriableError(`Unable to resolve topic id ${key} to a subscribed topic name`);
  }

  async #refreshTopicIdMapFromDescribe(): Promise<void> {
    const { coordinator, groupId } = this;
    if (!coordinator) return;

    const { groups } = await coordinator.shareGroupDescribe({
      groupIds: [groupId],
      includeAuthorizedOperations: false,
    });
    const group = groups.find((entry) => entry.groupId === groupId);
    if (!group) return;

    for (const member of group.members) {
      for (const { topicId, topicName } of member.assignment.topicPartitions) {
        if (topicName) this.#topicNameById.set(this.#topicIdKey(topicId), topicName);
      }
    }
  }

  #ensureMemberId(): string {
    if (!this.memberId) this.memberId = randomUUID();
    return this.memberId;
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
      throw new KafkaError(error as Error);
    }
  }
}
