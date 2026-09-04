import { supportsTransactionV2 } from '../../broker/capabilities';
import type { Broker } from '../../broker/index';
import type { Cluster, TopicOffsets } from '../../cluster/index';
import { INT_32_MAX_VALUE } from '../../constants';
import { KafkaNonRetriableError } from '../../errors';
import type { Logger } from '../../loggers/index';
import { COORDINATOR_TYPES } from '../../protocol/enums/coordinator-types';
import { retrier, type RetryOptions } from '../../retry/index';
import { Lock } from '../../utils/lock';
import { TransactionStateMachine, type TransitionEvent } from './transaction-state-machine';
import { TRANSACTION_STATES } from './transaction-states';

const NO_PRODUCER_ID = -1n;
const SEQUENCE_START = 0;

const INIT_PRODUCER_RETRIABLE_PROTOCOL_ERRORS = new Set([
  'NOT_COORDINATOR_FOR_GROUP',
  'GROUP_COORDINATOR_NOT_AVAILABLE',
  'GROUP_LOAD_IN_PROGRESS',
  // The producer might have crashed and never committed the transaction; retry the request so
  // Kafka can abort the current transaction.
  // https://github.com/apache/kafka/blob/201da0542726472d954080d54bc585b111aaf86f/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java#L1001-L1002
  'CONCURRENT_TRANSACTIONS',
]);

/** Retry while the coordinator is still loading; refresh the coordinator once it moves. */
const COMMIT_RETRIABLE_PROTOCOL_ERRORS = new Set(['UNKNOWN_TOPIC_OR_PARTITION', 'GROUP_LOAD_IN_PROGRESS']);
const COMMIT_STALE_COORDINATOR_PROTOCOL_ERRORS = new Set([
  'GROUP_COORDINATOR_NOT_AVAILABLE',
  'NOT_COORDINATOR_FOR_GROUP',
]);

export interface EosManagerTopicPartitions {
  topic: string;
  partitions: readonly { partition: number }[];
}

export interface EosManager {
  getProducerId: () => bigint;
  getProducerEpoch: () => number;
  getTransactionalId: () => string | undefined;
  initProducerId: () => Promise<void>;
  getSequence: (topic: string, partition: number) => number;
  updateSequence: (topic: string, partition: number, increment: number) => void;
  beginTransaction: () => void;
  addPartitionsToTransaction: (topicData: readonly EosManagerTopicPartitions[]) => Promise<void>;
  /** KIP-890: mark the current transaction abort-only after a `TRANSACTION_ABORTABLE` produce error. No-op outside an active transaction. */
  markTransactionAbortable: () => void;
  commit: () => Promise<void>;
  abort: () => Promise<void>;
  isInitialized: () => boolean;
  isTransactional: () => boolean;
  isInTransaction: () => boolean;
  /**
   * Serialize Produce for these topic-partitions so sequences stay monotonic.
   * Distinct partitions may overlap; the same partition is exclusive (≤ 1 in-flight).
   */
  acquirePartitionGates: (partitions: readonly EosManagerPartition[]) => Promise<void>;
  releasePartitionGates: (partitions: readonly EosManagerPartition[]) => Promise<void>;
  sendOffsets: (options: { consumerGroupId: string; topics: readonly TopicOffsets[] }) => Promise<void>;
}

export interface EosManagerPartition {
  topic: string;
  partition: number;
}

export interface EosManagerOptions {
  logger: Logger;
  cluster: Cluster;
  transactionTimeout?: number;
  transactional?: boolean;
  transactionalId?: string;
  retry?: RetryOptions;
}

/** Manages idempotent-producer and transaction behavior: producer id/epoch, sequence tracking, and the transaction lifecycle. */
export function createEosManager({
  logger,
  cluster,
  transactionTimeout = 60_000,
  transactional = false,
  transactionalId,
  retry: retryOptions,
}: EosManagerOptions): EosManager {
  if (transactional && !transactionalId) {
    throw new KafkaNonRetriableError('Cannot manage transactions without a transactionalId');
  }

  const retry = retrier(retryOptions);

  let producerId = NO_PRODUCER_ID;
  let producerEpoch = 0;

  /** Idempotent production tracks the next sequence number to send, per topic-partition. */
  const producerSequence = new Map<string, Map<number, number>>();

  /**
   * Serializes InitProducerId / epoch bump. Produce no longer takes a broker-wide
   * mutex; overlapping RPCs to one broker are allowed when they do not share a partition.
   */
  const initProducerIdLock = new Lock({ timeout: 0xffff, description: 'InitProducerId' });

  /**
   * Per-partition Produce gate. Same-partition Produce is exclusive so sequences stay
   * monotonic on mixed-partition RPCs. Distinct partitions on one broker may overlap.
   * The broker's idempotent window is 5; we keep 1 in-flight per partition rather than
   * pipelining the same partition without a single sender thread (OutOfOrderSequence).
   */
  const partitionGates = new Map<string, Lock>();

  /** Topic-partitions already known to be participating in the current transaction. */
  let transactionTopicPartitions = new Map<string, Set<number>>();

  /** Whether consumer-group offsets have been added to the current transaction. */
  let hasOffsetsAddedToTransaction = false;

  const stateMachine = new TransactionStateMachine({ logger });
  stateMachine.on('transition', ({ to }: TransitionEvent) => {
    if (to === TRANSACTION_STATES.READY) {
      transactionTopicPartitions = new Map();
      hasOffsetsAddedToTransaction = false;
    }
  });

  function findTransactionCoordinator(): Promise<Broker> {
    return cluster.findGroupCoordinator({ groupId: transactionalId!, coordinatorType: COORDINATOR_TYPES.TRANSACTION });
  }

  /** KIP-890 part 2: Produce v12+ (Kafka 4.0+) covers AddPartitionsToTxn and bumps the epoch every transaction. */
  function isTransactionV2Enabled(): boolean {
    const versions = cluster.brokerPool.versions;
    return versions != null && supportsTransactionV2(versions);
  }

  function transactionalGuard(): void {
    if (!transactional) {
      throw new KafkaNonRetriableError('Method unavailable if non-transactional');
    }
  }

  function isOngoing(): boolean {
    if (hasOffsetsAddedToTransaction) return true;
    for (const partitions of transactionTopicPartitions.values()) {
      if (partitions.size > 0) return true;
    }
    return false;
  }

  function getProducerId(): bigint {
    return producerId;
  }

  function getProducerEpoch(): number {
    return producerEpoch;
  }

  function getTransactionalId(): string | undefined {
    return transactionalId;
  }

  function isInitialized(): boolean {
    return producerId !== NO_PRODUCER_ID;
  }

  function isTransactional(): boolean {
    return transactional;
  }

  function isInTransaction(): boolean {
    return stateMachine.state() === TRANSACTION_STATES.TRANSACTING;
  }

  async function initProducerId(): Promise<void> {
    await initProducerIdLock.acquire();
    try {
      await retry(async (bail, retryCount, retryTime) => {
        try {
          await cluster.refreshMetadataIfNecessary();

          // If non-transactional, we can request the PID from any broker.
          const broker = transactional ? await findTransactionCoordinator() : await cluster.findControllerBroker();

          const result = await broker.initProducerId({
            transactionalId: transactional ? transactionalId! : null,
            transactionTimeout,
            producerId: producerId === NO_PRODUCER_ID ? -1n : producerId,
            producerEpoch: producerId === NO_PRODUCER_ID ? -1 : producerEpoch,
          });

          stateMachine.transitionTo(TRANSACTION_STATES.READY);
          producerId = result.producerId;
          producerEpoch = result.producerEpoch;
          producerSequence.clear();
          // Partition gates are left in place (not cleared): a gate is just a per-partition mutex,
          // and dropping it here would let a currently in-flight Produce - still holding the old
          // Lock object - get silently replaced by a fresh, already-unlocked one on its next
          // acquire/release, breaking the exclusivity guarantee across this epoch bump.

          logger.debug('Initialized producer id & epoch', { producerId: producerId.toString(), producerEpoch });
        } catch (e) {
          const error = e as Error & { type?: string };

          if (error.type && INIT_PRODUCER_RETRIABLE_PROTOCOL_ERRORS.has(error.type)) {
            if (error.type === 'CONCURRENT_TRANSACTIONS') {
              logger.debug('There is an ongoing transaction on this transactionId, retrying', {
                error: error.message,
                stack: error.stack,
                transactionalId,
                retryCount,
                retryTime,
              });
            }

            throw error;
          }

          bail(error);
        }
      });
    } finally {
      await initProducerIdLock.release();
    }
  }

  function getSequence(topic: string, partition: number): number {
    if (!isInitialized()) return SEQUENCE_START;

    let topicSequence = producerSequence.get(topic);
    if (!topicSequence) {
      topicSequence = new Map();
      producerSequence.set(topic, topicSequence);
    }

    let sequence = topicSequence.get(partition);
    if (sequence === undefined) {
      sequence = SEQUENCE_START;
      topicSequence.set(partition, sequence);
    }

    return sequence;
  }

  function updateSequence(topic: string, partition: number, increment: number): void {
    if (!isInitialized()) return;

    const previous = getSequence(topic, partition);
    let sequence = previous + increment;

    // Sequence is an Int32; rotate only after the last legal value (`2^31-1`) has been used.
    if (sequence > INT_32_MAX_VALUE) {
      logger.debug(`Sequence for ${topic} ${partition} exceeds max value (${sequence}). Rotating to 0.`);
      sequence = 0;
    }

    producerSequence.get(topic)!.set(partition, sequence);
  }

  function beginTransaction(): void {
    transactionalGuard();
    stateMachine.transitionTo(TRANSACTION_STATES.TRANSACTING);
  }

  async function addPartitionsToTransaction(topicData: readonly EosManagerTopicPartitions[]): Promise<void> {
    transactionalGuard();

    const newTopicPartitions = new Map<string, number[]>();

    for (const { topic, partitions } of topicData) {
      let addedPartitions = transactionTopicPartitions.get(topic);
      if (!addedPartitions) {
        addedPartitions = new Set();
        transactionTopicPartitions.set(topic, addedPartitions);
      }

      for (const { partition } of partitions) {
        if (!addedPartitions.has(partition)) {
          let pending = newTopicPartitions.get(topic);
          if (!pending) {
            pending = [];
            newTopicPartitions.set(topic, pending);
          }
          pending.push(partition);
        }
      }
    }

    const topics = [...newTopicPartitions.entries()].map(([topic, partitions]) => ({ topic, partitions }));

    // KIP-890 part 2: transaction V2 lets Produce itself cover AddPartitionsToTxn, so the
    // partitions are marked below without the extra round trip to the transaction coordinator.
    if (topics.length > 0 && !isTransactionV2Enabled()) {
      const broker = await findTransactionCoordinator();
      await broker.addPartitionsToTxn({ transactionalId: transactionalId!, producerId, producerEpoch, topics });
    }

    for (const { topic, partitions } of topics) {
      const addedPartitions = transactionTopicPartitions.get(topic)!;
      for (const partition of partitions) addedPartitions.add(partition);
    }
  }

  /** No-op outside TRANSACTING so a race with an in-flight commit/abort can't throw from an invalid transition. */
  function markTransactionAbortable(): void {
    if (!transactional) return;
    if (stateMachine.state() === TRANSACTION_STATES.TRANSACTING) {
      stateMachine.transitionTo(TRANSACTION_STATES.ABORTABLE);
    }
  }

  async function endTransaction(
    transactionResult: boolean,
    nextState: typeof TRANSACTION_STATES.COMMITTING | typeof TRANSACTION_STATES.ABORTING,
  ): Promise<void> {
    transactionalGuard();
    stateMachine.transitionTo(nextState);

    if (!isOngoing()) {
      logger.debug('No partitions or offsets registered, not sending EndTxn');
      stateMachine.transitionTo(TRANSACTION_STATES.READY);
      return;
    }

    const broker = await findTransactionCoordinator();
    await broker.endTxn({ producerId, producerEpoch, transactionalId: transactionalId!, transactionResult });
    stateMachine.transitionTo(TRANSACTION_STATES.READY);

    // KIP-890 part 2: transaction V2 bumps the producer epoch on every transaction (not just on
    // fencing recovery), so the next transaction starts from a fresh epoch.
    if (isTransactionV2Enabled()) {
      await initProducerId();
    }
  }

  async function commit(): Promise<void> {
    await endTransaction(true, TRANSACTION_STATES.COMMITTING);
  }

  async function abort(): Promise<void> {
    await endTransaction(false, TRANSACTION_STATES.ABORTING);
  }

  function partitionGateKey(topic: string, partition: number): string {
    return `${topic}\0${partition}`;
  }

  function comparePartitions(a: EosManagerPartition, b: EosManagerPartition): number {
    const byTopic = a.topic < b.topic ? -1 : a.topic > b.topic ? 1 : 0;
    return byTopic !== 0 ? byTopic : a.partition - b.partition;
  }

  function uniqueSortedPartitions(partitions: readonly EosManagerPartition[]): EosManagerPartition[] {
    const seen = new Set<string>();
    const unique: EosManagerPartition[] = [];
    for (const entry of partitions) {
      const key = partitionGateKey(entry.topic, entry.partition);
      if (seen.has(key)) continue;
      seen.add(key);
      unique.push(entry);
    }
    unique.sort(comparePartitions);
    return unique;
  }

  function partitionLock(topic: string, partition: number): Lock {
    const key = partitionGateKey(topic, partition);
    let lock = partitionGates.get(key);
    if (!lock) {
      lock = new Lock({ timeout: 0xffff, description: `idempotent produce ${key}` });
      partitionGates.set(key, lock);
    }
    return lock;
  }

  async function acquirePartitionGates(partitions: readonly EosManagerPartition[]): Promise<void> {
    if (!isInitialized()) return;

    const unique = uniqueSortedPartitions(partitions);
    const acquired: EosManagerPartition[] = [];
    try {
      for (const entry of unique) {
        await partitionLock(entry.topic, entry.partition).acquire();
        acquired.push(entry);
      }
    } catch (error) {
      for (let i = acquired.length - 1; i >= 0; i--) {
        const entry = acquired[i];
        if (!entry) continue;
        await partitionLock(entry.topic, entry.partition).release();
      }
      throw error;
    }
  }

  async function releasePartitionGates(partitions: readonly EosManagerPartition[]): Promise<void> {
    if (!isInitialized()) return;

    const unique = uniqueSortedPartitions(partitions);
    for (let i = unique.length - 1; i >= 0; i--) {
      const entry = unique[i];
      if (!entry) continue;
      await partitionLock(entry.topic, entry.partition).release();
    }
  }

  async function sendOffsets({
    consumerGroupId,
    topics,
  }: {
    consumerGroupId: string;
    topics: readonly TopicOffsets[];
  }): Promise<void> {
    const transactionCoordinator = await findTransactionCoordinator();

    await transactionCoordinator.addOffsetsToTxn({
      transactionalId: transactionalId!,
      producerId,
      producerEpoch,
      groupId: consumerGroupId,
    });

    hasOffsetsAddedToTransaction = true;

    let groupCoordinator = await cluster.findGroupCoordinator({
      groupId: consumerGroupId,
      coordinatorType: COORDINATOR_TYPES.GROUP,
    });

    const requestTopics = topics.map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.map(({ partition, offset }) => ({ partition, offset, metadata: null })),
    }));

    await retry(async (bail, retryCount, retryTime) => {
      try {
        await groupCoordinator.txnOffsetCommit({
          transactionalId: transactionalId!,
          producerId,
          producerEpoch,
          groupId: consumerGroupId,
          topics: requestTopics,
        });
      } catch (e) {
        const error = e as Error & { type?: string; code?: string };

        if (error.type && COMMIT_RETRIABLE_PROTOCOL_ERRORS.has(error.type)) {
          logger.debug('Group coordinator is not ready yet, retrying', {
            error: error.message,
            stack: error.stack,
            transactionalId,
            retryCount,
            retryTime,
          });
          throw error;
        }

        if ((error.type && COMMIT_STALE_COORDINATOR_PROTOCOL_ERRORS.has(error.type)) || error.code === 'ECONNREFUSED') {
          logger.debug('Invalid group coordinator, finding new group coordinator and retrying', {
            error: error.message,
            stack: error.stack,
            transactionalId,
            retryCount,
            retryTime,
          });
          groupCoordinator = await cluster.findGroupCoordinator({
            groupId: consumerGroupId,
            coordinatorType: COORDINATOR_TYPES.GROUP,
          });
          throw error;
        }

        bail(error);
      }
    });
  }

  return stateMachine.createGuarded(
    {
      getProducerId,
      getProducerEpoch,
      getTransactionalId,
      initProducerId,
      getSequence,
      updateSequence,
      beginTransaction,
      addPartitionsToTransaction,
      markTransactionAbortable,
      commit,
      abort,
      isInitialized,
      isTransactional,
      isInTransaction,
      acquirePartitionGates,
      releasePartitionGates,
      sendOffsets,
    },
    {
      initProducerId: { legalStates: [TRANSACTION_STATES.UNINITIALIZED, TRANSACTION_STATES.READY] },
      beginTransaction: { legalStates: [TRANSACTION_STATES.READY], async: false },
      addPartitionsToTransaction: { legalStates: [TRANSACTION_STATES.TRANSACTING] },
      sendOffsets: { legalStates: [TRANSACTION_STATES.TRANSACTING] },
      commit: { legalStates: [TRANSACTION_STATES.TRANSACTING] },
      abort: { legalStates: [TRANSACTION_STATES.TRANSACTING, TRANSACTION_STATES.ABORTABLE] },
    },
  );
}
