import type { Broker } from '../../broker/index.js';
import type { Cluster, TopicOffsets } from '../../cluster/index.js';
import { INT_32_MAX_VALUE } from '../../constants.js';
import { KafkaJSNonRetriableError } from '../../errors.js';
import type { Logger } from '../../loggers/index.js';
import { COORDINATOR_TYPES } from '../../protocol/enums/coordinator-types.js';
import { retrier } from '../../retry/index.js';
import { Lock } from '../../utils/lock.js';
import { TransactionStateMachine, type TransitionEvent } from './transaction-state-machine.js';
import { TRANSACTION_STATES } from './transaction-states.js';

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

/**
 * kafkajs's own equivalent lists here are `['UNKNOWN_TOPIC_OR_PARTITION', 'COORDINATOR_LOAD_IN_PROGRESS']`
 * and `['COORDINATOR_NOT_AVAILABLE', 'NOT_COORDINATOR']` - but those newer error-code names don't
 * exist anywhere in kafkajs's own error table (`protocol/error.js` only ever produces
 * `GROUP_LOAD_IN_PROGRESS`/`GROUP_COORDINATOR_NOT_AVAILABLE`/`NOT_COORDINATOR_FOR_GROUP` for these
 * codes), so half of each list can never match a real error there. Using the names this port's own
 * `error-codes.ts` actually produces restores the intended "retry while the coordinator is still
 * loading, look up a fresh coordinator once it moves" behavior.
 */
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
  commit: () => Promise<void>;
  abort: () => Promise<void>;
  isInitialized: () => boolean;
  isTransactional: () => boolean;
  isInTransaction: () => boolean;
  acquireBrokerLock: (broker: Broker) => Promise<void>;
  releaseBrokerLock: (broker: Broker) => Promise<void>;
  sendOffsets: (options: { consumerGroupId: string; topics: readonly TopicOffsets[] }) => Promise<void>;
}

export interface EosManagerOptions {
  logger: Logger;
  cluster: Cluster;
  transactionTimeout?: number;
  transactional?: boolean;
  transactionalId?: string;
}

/** Manages idempotent-producer and transaction behavior: producer id/epoch, sequence tracking, and the transaction lifecycle. */
export function createEosManager({
  logger,
  cluster,
  transactionTimeout = 60_000,
  transactional = false,
  transactionalId,
}: EosManagerOptions): EosManager {
  if (transactional && !transactionalId) {
    throw new KafkaJSNonRetriableError('Cannot manage transactions without a transactionalId');
  }

  const retry = retrier();

  let producerId = NO_PRODUCER_ID;
  let producerEpoch = 0;

  /** Idempotent production tracks the next sequence number to send, per topic-partition. */
  const producerSequence = new Map<string, Map<number, number>>();

  /** Serializes requests per broker so sequence-number bookkeeping never races. */
  const brokerMutexLocks = new Map<number, Lock>();

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

  function transactionalGuard(): void {
    if (!transactional) {
      throw new KafkaJSNonRetriableError('Method unavailable if non-transactional');
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
    await retry(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadataIfNecessary();

        // If non-transactional, we can request the PID from any broker.
        const broker = transactional ? await findTransactionCoordinator() : await cluster.findControllerBroker();

        const result = await broker.initProducerId({
          transactionalId: transactional ? transactionalId! : null,
          transactionTimeout,
        });

        stateMachine.transitionTo(TRANSACTION_STATES.READY);
        producerId = result.producerId;
        producerEpoch = result.producerEpoch;
        producerSequence.clear();
        brokerMutexLocks.clear();

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

    // Sequence is defined as Int32 in the record batch, so theoretically we should rotate here.
    if (sequence >= INT_32_MAX_VALUE) {
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

    if (topics.length > 0) {
      const broker = await findTransactionCoordinator();
      await broker.addPartitionsToTxn({ transactionalId: transactionalId!, producerId, producerEpoch, topics });
    }

    for (const { topic, partitions } of topics) {
      const addedPartitions = transactionTopicPartitions.get(topic)!;
      for (const partition of partitions) addedPartitions.add(partition);
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
  }

  async function commit(): Promise<void> {
    await endTransaction(true, TRANSACTION_STATES.COMMITTING);
  }

  async function abort(): Promise<void> {
    await endTransaction(false, TRANSACTION_STATES.ABORTING);
  }

  async function acquireBrokerLock(broker: Broker): Promise<void> {
    if (!isInitialized()) return;

    const nodeId = broker.nodeId!;
    let lock = brokerMutexLocks.get(nodeId);
    if (!lock) {
      lock = new Lock({ timeout: 0xffff });
      brokerMutexLocks.set(nodeId, lock);
    }

    await lock.acquire();
  }

  async function releaseBrokerLock(broker: Broker): Promise<void> {
    if (!isInitialized()) return;
    await brokerMutexLocks.get(broker.nodeId!)?.release();
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
      commit,
      abort,
      isInitialized,
      isTransactional,
      isInTransaction,
      acquireBrokerLock,
      releaseBrokerLock,
      sendOffsets,
    },
    {
      initProducerId: { legalStates: [TRANSACTION_STATES.UNINITIALIZED, TRANSACTION_STATES.READY] },
      beginTransaction: { legalStates: [TRANSACTION_STATES.READY], async: false },
      addPartitionsToTransaction: { legalStates: [TRANSACTION_STATES.TRANSACTING] },
      sendOffsets: { legalStates: [TRANSACTION_STATES.TRANSACTING] },
      commit: { legalStates: [TRANSACTION_STATES.TRANSACTING] },
      abort: { legalStates: [TRANSACTION_STATES.TRANSACTING] },
    },
  );
}
