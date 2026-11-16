import type { Broker } from '../broker/index';
import type { Cluster } from '../cluster/index';
import type { TopicPartitions } from '../consumer/types';
import { HeartbeatMember } from '../consumer/membership/heartbeat-member';
import { TopicIdNames, type DescribedTopic } from '../consumer/membership/topic-id-names';
import { KafkaError, KafkaNonRetriableError, type KafkaProtocolError } from '../errors';
import { InstrumentationEventEmitter } from '../instrumentation/emitter';
import type { Logger } from '../loggers/index';
import { createErrorFromCode, failure, staleMetadata } from '../protocol/error-codes';
import type { ShareAcknowledgeTopicInput } from '../protocol/requests/share-acknowledge/index';
import {
  SHARE_ACQUIRE_MODE,
  SHARE_SESSION_CLOSE_EPOCH,
  type ShareAcquireMode,
  type ShareFetchAcknowledgementBatchInput,
  type ShareFetchResponseV1Body,
  type ShareFetchTopicInput,
} from '../protocol/requests/share-fetch/index';
import { SHARE_GROUP_LEAVE_EPOCH } from '../protocol/requests/share-group-heartbeat/index';
import type { ShareGroupHeartbeatResponseV1Body } from '../protocol/requests/share-group-heartbeat/v1/response';
import { retrier, type RetryOptions } from '../retry/index';
import { sleep } from '../utils/wait';
import { SHARE_ACKNOWLEDGE_TYPE, type ShareAcknowledgeType } from './acknowledge-types';
import {
  AcknowledgementLedger,
  hasRenewAcknowledgement,
  toAcknowledgeEventTopics,
  toAcknowledgementBatches,
  type TakenAcknowledgements,
} from './acknowledgement-ledger';
import { ACKNOWLEDGE } from './instrumentation-events';
import { ShareBatch } from './share-batch';
import { ShareSessionHandler, type ShareSessionTopic } from './share-session';
import { ShareGroupDescribe } from '../protocol/requests/lazy-families';
import { ShareAcknowledge } from '../protocol/requests/share-acknowledge/index';
import { ShareFetch } from '../protocol/requests/share-fetch/index';
import { ShareGroupHeartbeat } from '../protocol/requests/share-group-heartbeat/index';

const JOIN_DEADLINE_MS = 30_000;
const DEFAULT_MAX_WAIT_MS = 5000;
const DEFAULT_MIN_BYTES = 1;
const DEFAULT_MAX_BYTES = 50 * 1024 * 1024;
const DEFAULT_MAX_RECORDS = 500;
const DEFAULT_BATCH_SIZE = 100;

export interface ShareGroupOptions {
  cluster: Cluster;
  groupId: string;
  logger: Logger;
  retry?: RetryOptions;
  rackId?: string;
  maxWaitTimeInMs?: number;
  minBytes?: number;
  maxBytes?: number;
  maxRecords?: number;
  batchSize?: number;
  shareAcquireMode?: ShareAcquireMode;
  instrumentationEmitter?: InstrumentationEventEmitter | null;
}

/** A topic a request named, so its response's topic id can be mapped back to the name. */
interface RequestedTopic {
  topic: string;
  topicId: Buffer;
}

/** Response topic ids are those of the request that carried them; any other id is a broker error. */
function requestedTopicName(requestedTopics: readonly RequestedTopic[], topicId: Buffer): string {
  for (const { topic, topicId: requestedId } of requestedTopics) {
    if (requestedId.equals(topicId)) return topic;
  }
  throw new KafkaError('Unknown topic id in share response');
}

/** The same topics with no acknowledgements, for a retry that must not replay them. */
function withoutAcknowledgements(topics: readonly ShareFetchTopicInput[]): ShareFetchTopicInput[] {
  return topics.map(({ topicId, partitions }) => ({
    topicId,
    partitions: partitions.map(({ partitionIndex }) => ({ partitionIndex, acknowledgementBatches: [] })),
  }));
}

/**
 * Share-group membership via ShareGroupHeartbeat, and the ShareFetch and ShareAcknowledge requests
 * that acquire and acknowledge its records (KIP-932).
 */
export class ShareGroup {
  readonly groupId: string;
  readonly cluster: Cluster;
  readonly #logger: Logger;
  readonly rackId: string;
  readonly #maxWaitTimeInMs: number;
  readonly #minBytes: number;
  readonly #maxBytes: number;
  readonly #maxRecords: number;
  readonly #batchSize: number;
  readonly #shareAcquireMode: ShareAcquireMode;
  readonly #instrumentationEmitter: InstrumentationEventEmitter;

  /** Member id, epoch and heartbeat state, shared with the consumer group protocol (KIP-848). */
  readonly member = new HeartbeatMember();
  coordinator: Broker | null = null;
  topicsSubscribed: string[] = [];
  assignment: TopicPartitions[] = [];
  shuttingDown = false;
  readonly #topicNames: TopicIdNames;
  readonly #retrier: ReturnType<typeof retrier>;
  /** One share session per leader node. */
  readonly #shareSessions = new Map<string, ShareSessionHandler>();
  readonly #acknowledgements = new AcknowledgementLedger();
  /** Set when a partition reported stale leadership; the next fetch forces a metadata refresh. */
  #metadataRefreshNeeded = false;

  constructor({
    cluster,
    groupId,
    logger,
    retry,
    rackId = '',
    maxWaitTimeInMs = DEFAULT_MAX_WAIT_MS,
    minBytes = DEFAULT_MIN_BYTES,
    maxBytes = DEFAULT_MAX_BYTES,
    maxRecords = DEFAULT_MAX_RECORDS,
    batchSize = DEFAULT_BATCH_SIZE,
    shareAcquireMode = SHARE_ACQUIRE_MODE.BATCH_OPTIMIZED,
    instrumentationEmitter,
  }: ShareGroupOptions) {
    this.cluster = cluster;
    this.groupId = groupId;
    this.#logger = logger.namespace('ShareGroup');
    this.#retrier = retrier(retry);
    this.rackId = rackId;
    this.#maxWaitTimeInMs = maxWaitTimeInMs;
    this.#minBytes = minBytes;
    this.#maxBytes = maxBytes;
    this.#maxRecords = maxRecords;
    this.#batchSize = batchSize;
    this.#shareAcquireMode = shareAcquireMode;
    this.#instrumentationEmitter = instrumentationEmitter ?? new InstrumentationEventEmitter();
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
      await this.coordinator.send(ShareGroupHeartbeat, {
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
      return this.coordinator.send(ShareGroupHeartbeat, {
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

    const { groups } = await coordinator.send(ShareGroupDescribe, {
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

  /** Queues `acknowledgeType` for the batch's acquired records; the next request to its leader sends it. */
  acknowledge(batch: ShareBatch, acknowledgeType: ShareAcknowledgeType): void {
    this.#acknowledgements.queue(batch.topic, batch.partition, batch.acquiredRecords, acknowledgeType);
  }

  /**
   * Sends one ShareFetch to `nodeId` for the assigned partitions it leads, carrying their pending
   * acknowledgements, and returns the acquired records. A retriable partition error skips that
   * partition; any other partition error is thrown after the healthy partitions' records are released.
   */
  async fetch(nodeId: string): Promise<ShareBatch[]> {
    if (this.#metadataRefreshNeeded) {
      await this.cluster.refreshMetadata();
      this.#metadataRefreshNeeded = false;
    } else {
      await this.cluster.refreshMetadataIfNecessary();
    }
    const topicPartitions = this.filterPartitionsByNode(nodeId, this.assignment);
    if (topicPartitions.length === 0) return [];

    const { memberId } = this;
    if (!memberId) return [];

    const session = this.#shareSessionFor(nodeId);
    // An initial-epoch ShareFetch may not carry acknowledgements, and opening a new session
    // releases whatever the previous one acquired, so acknowledgements still pending for it are dropped.
    const carriesAcknowledgements = session.hasSession();
    const sessionTopics: ShareSessionTopic[] = [];
    const topics: ShareFetchTopicInput[] = [];
    const taken: TakenAcknowledgements[] = [];
    for (const { topic, partitions } of topicPartitions) {
      const topicId = this.cluster.findTopicId(topic);
      if (!topicId) continue;
      sessionTopics.push({ topic, topicId, partitions });
      topics.push({
        topicId,
        partitions: partitions.map((partitionIndex) => {
          if (carriesAcknowledgements) {
            const acknowledgementBatches = this.#takeAcknowledgementBatches(topic, partitionIndex, taken) ?? [];
            return { partitionIndex, acknowledgementBatches };
          }
          this.#acknowledgements.take(topic, partitionIndex);
          return { partitionIndex, acknowledgementBatches: [] };
        }),
      });
    }
    if (topics.length === 0) return [];

    const isRenewAck = hasRenewAcknowledgement(taken);
    let attempts = 0;
    let response: ShareFetchResponseV1Body;
    try {
      const broker = await this.cluster.findBroker({ nodeId });
      response = await this.#retrier(() => {
        // A failed attempt may already have been applied (epoch bumped, acknowledgements
        // recorded), so replaying it is ambiguous; retries open a fresh session without acknowledgements.
        const isRetry = attempts++ > 0;
        if (isRetry) session.reset();
        const { shareSessionEpoch, forgottenTopics } = session.buildRequest(sessionTopics);
        return broker.send(ShareFetch, {
          groupId: this.groupId,
          memberId,
          shareSessionEpoch,
          maxWaitMs: this.#maxWaitTimeInMs,
          minBytes: this.#minBytes,
          maxBytes: this.#maxBytes,
          maxRecords: this.#maxRecords,
          batchSize: this.#batchSize,
          shareAcquireMode: this.#shareAcquireMode,
          isRenewAck: !isRetry && isRenewAck,
          topics: isRetry ? withoutAcknowledgements(topics) : topics,
          forgottenTopics,
        });
      });
    } catch (error) {
      if (attempts === 0) {
        this.#acknowledgements.restore(taken);
        throw error;
      }
      // Whether the broker applied any attempt is unknown, so the session no longer matches what it
      // has: the next fetch for this node opens a new one. The acknowledgements taken for this
      // request cannot ride on that request and are dropped with the session.
      session.reset();
      throw error;
    }

    // The request succeeded, so the session advanced even if a partition below reports an error.
    session.handleResponse();
    if (taken.length > 0 && attempts === 1) this.#emitAcknowledge(nodeId, memberId, taken);

    const batches: ShareBatch[] = [];
    let fatalError: KafkaProtocolError | undefined;
    for (const { topicId, partitions } of response.responses) {
      const topic = requestedTopicName(sessionTopics, topicId);
      for (const partitionData of partitions) {
        const { partitionIndex: partition, errorCode, acknowledgeErrorCode } = partitionData;
        if (failure(acknowledgeErrorCode)) this.#warnAcknowledgeFailure(topic, partition, acknowledgeErrorCode);
        if (failure(errorCode)) {
          fatalError ??= this.#handlePartitionFetchError(topic, partition, errorCode);
          continue;
        }
        batches.push(
          new ShareBatch(topic, {
            partition,
            messages: partitionData.records,
            acquiredRecords: partitionData.acquiredRecords,
          }),
        );
      }
    }
    if (fatalError) {
      // Release what the healthy partitions acquired rather than leave it locked until it times out.
      for (const batch of batches) this.acknowledge(batch, SHARE_ACKNOWLEDGE_TYPE.RELEASE);
      throw fatalError;
    }
    return batches;
  }

  /**
   * Flushes the pending acknowledgements of each node's partitions with a closing ShareAcknowledge
   * and ends every share session. Best-effort: a node that fails is skipped, and whatever is still
   * pending afterwards is dropped.
   */
  async closeShareSessions(): Promise<void> {
    try {
      const { memberId } = this;
      if (!memberId) return;

      const nodeIds = new Set<string>();
      for (const [nodeId, session] of this.#shareSessions) {
        if (session.hasSession()) nodeIds.add(nodeId);
      }
      for (const nodeId of this.getNodeIds()) nodeIds.add(nodeId);
      for (const nodeId of nodeIds) {
        try {
          await this.#closeShareSession(nodeId, memberId);
        } catch {
          // Closing is best-effort; leave() still runs after this.
        }
      }
    } finally {
      this.#acknowledgements.clear();
      this.#shareSessions.clear();
    }
  }

  async #closeShareSession(nodeId: string, memberId: string): Promise<void> {
    const requestedTopics: RequestedTopic[] = [];
    const topics: ShareAcknowledgeTopicInput[] = [];
    const taken: TakenAcknowledgements[] = [];
    for (const { topic, partitions } of this.filterPartitionsByNode(nodeId, this.assignment)) {
      const topicId = this.cluster.findTopicId(topic);
      if (!topicId) continue;
      const acknowledgedPartitions: ShareAcknowledgeTopicInput['partitions'] = [];
      for (const partitionIndex of partitions) {
        const acknowledgementBatches = this.#takeAcknowledgementBatches(topic, partitionIndex, taken);
        if (acknowledgementBatches) acknowledgedPartitions.push({ partitionIndex, acknowledgementBatches });
      }
      if (acknowledgedPartitions.length === 0) continue;
      requestedTopics.push({ topic, topicId });
      topics.push({ topicId, partitions: acknowledgedPartitions });
    }

    const broker = await this.cluster.findBroker({ nodeId });
    const response = await broker.send(ShareAcknowledge, {
      groupId: this.groupId,
      memberId,
      shareSessionEpoch: SHARE_SESSION_CLOSE_EPOCH,
      isRenewAck: hasRenewAcknowledgement(taken),
      topics,
    });

    if (taken.length > 0) this.#emitAcknowledge(nodeId, memberId, taken);
    for (const { topicId, partitions } of response.responses) {
      const topic = requestedTopicName(requestedTopics, topicId);
      for (const { partitionIndex, errorCode } of partitions) {
        if (failure(errorCode)) this.#warnAcknowledgeFailure(topic, partitionIndex, errorCode);
      }
    }
  }

  #shareSessionFor(nodeId: string): ShareSessionHandler {
    let session = this.#shareSessions.get(nodeId);
    if (!session) {
      session = new ShareSessionHandler();
      this.#shareSessions.set(nodeId, session);
    }
    return session;
  }

  /** Takes the partition's pending acknowledgements into `taken` and returns them as request batches. */
  #takeAcknowledgementBatches(
    topic: string,
    partition: number,
    taken: TakenAcknowledgements[],
  ): ShareFetchAcknowledgementBatchInput[] | undefined {
    const acknowledgements = this.#acknowledgements.take(topic, partition);
    if (!acknowledgements) return undefined;
    taken.push({ topic, partition, acknowledgements });
    return toAcknowledgementBatches(acknowledgements);
  }

  #emitAcknowledge(nodeId: string, memberId: string, taken: readonly TakenAcknowledgements[]): void {
    this.#instrumentationEmitter.emit(ACKNOWLEDGE, {
      groupId: this.groupId,
      memberId,
      nodeId,
      topics: toAcknowledgeEventTopics(taken),
    });
  }

  /** Like Java's AcknowledgementCommitCallback: a failed acknowledgement is reported, not fatal. */
  #warnAcknowledgeFailure(topic: string, partition: number, errorCode: number): void {
    this.#logger.warn('Share acknowledgement failed', {
      groupId: this.groupId,
      topic,
      partition,
      error: createErrorFromCode(errorCode).message,
    });
  }

  /**
   * A retriable error skips only this partition this round; anything else is returned for the
   * caller to throw once the rest of the response is handled.
   */
  #handlePartitionFetchError(topic: string, partition: number, errorCode: number): KafkaProtocolError | undefined {
    const error = createErrorFromCode(errorCode, { topic, partition });
    if (!error.retriable) return error;
    if (staleMetadata(error)) this.#metadataRefreshNeeded = true;
    this.#logger.warn('Share fetch failed for partition, retrying', {
      groupId: this.groupId,
      topic,
      partition,
      error: error.message,
    });
    return undefined;
  }
}
