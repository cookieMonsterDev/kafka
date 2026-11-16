import { supportsTransactionV2 } from '../../broker/capabilities';
import type { Broker } from '../../broker/index';
import type { Cluster, TopicOffsets } from '../../cluster/index';
import { KafkaNonRetriableError, KafkaProtocolError } from '../../errors';
import type { Logger } from '../../loggers/index';
import { COORDINATOR_TYPES } from '../../protocol/enums/coordinator-types';
import { retrier, type RetryOptions } from '../../retry/index';
import { createIdempotenceLedger, type IdempotenceLedger, type ProducerIdentity } from '../idempotence-ledger';
import { TransactionStateMachine, type TransitionEvent } from './transaction-state-machine';
import { TRANSACTION_STATES } from './transaction-states';

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

/** EndTxn errors meaning a newer producer took over this transactionalId (Java treats them as fatal). */
const FENCED_PROTOCOL_ERRORS = new Set(['PRODUCER_FENCED', 'INVALID_PRODUCER_EPOCH']);

export interface EosManagerTopicPartitions {
  topic: string;
  partitions: readonly { partition: number }[];
}

export interface EosManager {
  /** Producer id, epoch, sequences and partition gates; a requested epoch bump waits while a transaction is open. */
  ledger: IdempotenceLedger;
  getTransactionalId: () => string | undefined;
  initProducerId: () => Promise<void>;
  beginTransaction: () => void;
  addPartitionsToTransaction: (topicData: readonly EosManagerTopicPartitions[]) => Promise<void>;
  /** KIP-890: mark the current transaction abort-only after a `TRANSACTION_ABORTABLE` produce error. No-op outside an active transaction. */
  markTransactionAbortable: () => void;
  commit: () => Promise<void>;
  abort: () => Promise<void>;
  isInitialized: () => boolean;
  isTransactional: () => boolean;
  isInTransaction: () => boolean;
  /** Fenced by a newer producer with the same transactionalId (`FATAL_ERROR`); nothing works anymore. */
  isFenced: () => boolean;
  sendOffsets: (options: { consumerGroupId: string; topics: readonly TopicOffsets[] }) => Promise<void>;
}

export interface EosManagerOptions {
  logger: Logger;
  cluster: Cluster;
  transactionTimeout?: number;
  transactional?: boolean;
  transactionalId?: string;
  retry?: RetryOptions;
}

/** Manages the transaction lifecycle on top of the idempotence ledger, which it feeds InitProducerId. */
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

  const ledger = createIdempotenceLedger({
    logger,
    initProducerId: sendInitProducerId,
    canBumpEpoch: () => stateMachine.state() === TRANSACTION_STATES.READY,
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

  function getTransactionalId(): string | undefined {
    return transactionalId;
  }

  function isTransactional(): boolean {
    return transactional;
  }

  function isInTransaction(): boolean {
    return stateMachine.state() === TRANSACTION_STATES.TRANSACTING;
  }

  function isFenced(): boolean {
    return stateMachine.state() === TRANSACTION_STATES.FATAL_ERROR;
  }

  function sendInitProducerId(current: ProducerIdentity): Promise<ProducerIdentity> {
    return retry(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadataIfNecessary();

        // If non-transactional, we can request the producer id from any broker.
        const broker = transactional ? await findTransactionCoordinator() : await cluster.findControllerBroker();

        const { producerId, producerEpoch } = await broker.initProducerId({
          transactionalId: transactional ? transactionalId! : null,
          transactionTimeout,
          producerId: current.producerId,
          producerEpoch: current.producerEpoch,
        });

        stateMachine.transitionTo(TRANSACTION_STATES.READY);
        return { producerId, producerEpoch };
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
        // Never used: `bail` has already rejected the retry.
        return current;
      }
    });
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
      await broker.addPartitionsToTxn({
        transactionalId: transactionalId!,
        producerId: ledger.getProducerId(),
        producerEpoch: ledger.getProducerEpoch(),
        topics,
      });
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
    // A previous attempt that failed leaves the machine in `nextState`, so the caller can resend EndTxn.
    if (stateMachine.state() !== nextState) stateMachine.transitionTo(nextState);

    if (!isOngoing()) {
      logger.debug('No partitions or offsets registered, not sending EndTxn');
      stateMachine.transitionTo(TRANSACTION_STATES.READY);
      return;
    }

    await retry(async (bail, retryCount, retryTime) => {
      try {
        // Looked up on every attempt so a moved coordinator (NOT_COORDINATOR, disconnect) is re-discovered.
        const broker = await findTransactionCoordinator();
        await broker.endTxn({
          producerId: ledger.getProducerId(),
          producerEpoch: ledger.getProducerEpoch(),
          transactionalId: transactionalId!,
          transactionResult,
        });
      } catch (e) {
        const error = e as Error & { retriable?: boolean };
        if (!error.retriable) {
          bail(error);
          return;
        }
        logger.debug('EndTxn failed, retrying', { error: error.message, transactionalId, retryCount, retryTime });
        throw error;
      }
    }).catch((error: unknown) => {
      // Anything but a broker rejection (exhausted retries, a lost connection) keeps `nextState` so
      // the caller can resend EndTxn; a rejected commit can still be aborted.
      if (error instanceof KafkaProtocolError && !error.retriable) {
        if (FENCED_PROTOCOL_ERRORS.has(error.type ?? '')) {
          logger.error('Producer was fenced by a newer instance; it cannot be used anymore', { transactionalId });
          stateMachine.transitionTo(TRANSACTION_STATES.FATAL_ERROR);
        } else if (transactionResult) {
          stateMachine.transitionTo(TRANSACTION_STATES.ABORTABLE);
        }
      }
      throw error;
    });
    stateMachine.transitionTo(TRANSACTION_STATES.READY);

    // KIP-890 part 2: transaction V2 bumps the producer epoch on every transaction (not just on
    // fencing recovery), so the next transaction starts from a fresh epoch.
    if (isTransactionV2Enabled() || ledger.isEpochBumpRequested()) {
      // The transaction has already ended, so a failed bump must not fail it; the next transaction
      // retries the bump before it begins.
      try {
        await ledger.initProducerId();
      } catch (error) {
        logger.warn('Failed to bump the producer epoch after ending the transaction; retrying later', {
          error: (error as Error).message,
          transactionalId,
        });
        ledger.requestEpochBump();
      }
    }
  }

  async function commit(): Promise<void> {
    await endTransaction(true, TRANSACTION_STATES.COMMITTING);
  }

  async function abort(): Promise<void> {
    await endTransaction(false, TRANSACTION_STATES.ABORTING);
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
      producerId: ledger.getProducerId(),
      producerEpoch: ledger.getProducerEpoch(),
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
          producerId: ledger.getProducerId(),
          producerEpoch: ledger.getProducerEpoch(),
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
      ledger,
      getTransactionalId,
      initProducerId: ledger.initProducerId,
      beginTransaction,
      addPartitionsToTransaction,
      markTransactionAbortable,
      commit,
      abort,
      isInitialized: ledger.isInitialized,
      isTransactional,
      isInTransaction,
      isFenced,
      sendOffsets,
    },
    {
      initProducerId: { legalStates: [TRANSACTION_STATES.UNINITIALIZED, TRANSACTION_STATES.READY] },
      beginTransaction: { legalStates: [TRANSACTION_STATES.READY], async: false },
      addPartitionsToTransaction: { legalStates: [TRANSACTION_STATES.TRANSACTING] },
      sendOffsets: { legalStates: [TRANSACTION_STATES.TRANSACTING] },
      commit: { legalStates: [TRANSACTION_STATES.TRANSACTING, TRANSACTION_STATES.COMMITTING] },
      abort: {
        legalStates: [TRANSACTION_STATES.TRANSACTING, TRANSACTION_STATES.ABORTABLE, TRANSACTION_STATES.ABORTING],
      },
    },
  );
}
