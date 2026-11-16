import { supportsTransactionV2 } from '../../broker/capabilities';
import type { Broker } from '../../broker/index';
import type { Cluster, TopicOffsets } from '../../cluster/index';
import {
  COORDINATOR_LOADING_ERRORS,
  isProducerFenced,
  STALE_METADATA_ERRORS,
  unionOf,
} from '../../cluster/error-classifier';
import { sendInitProducerId } from '../../cluster/init-producer-id';
import { coordinatorTarget } from '../../cluster/run';
import { KafkaNonRetriableError, KafkaProtocolError } from '../../errors';
import type { Logger } from '../../loggers/index';
import { COORDINATOR_TYPES } from '../../protocol/enums/coordinator-types';
import type { Retrier } from '../../retry/index';
import { createIdempotenceLedger, type IdempotenceLedger } from '../idempotence-ledger';
import type { OpenTransaction, TransactionTopicPartitions } from '../send-messages';
import type { ProducerBatch, ProducerRecord, RecordMetadata, Transaction } from '../types';
import { FENCED_PRODUCER_MESSAGE, TransactionStateMachine, type TransitionEvent } from './transaction-state-machine';
import { TRANSACTION_STATES, type TransactionState } from './transaction-states';
import { AddOffsetsToTxn } from '../../protocol/requests/add-offsets-to-txn/index';
import { AddPartitionsToTxn } from '../../protocol/requests/add-partitions-to-txn/index';
import { EndTxn } from '../../protocol/requests/end-txn/index';
import { TxnOffsetCommit } from '../../protocol/requests/txn-offset-commit/index';

/**
 * TxnOffsetCommit also retries while the group coordinator is still loading, or does not know a
 * topic yet; a moved coordinator is looked up again by `Cluster.run`.
 */
const TXN_OFFSET_COMMIT_RETRY_ON = unionOf(COORDINATOR_LOADING_ERRORS, STALE_METADATA_ERRORS);

type TransactionCall = 'send' | 'commit' | 'abort';

/** The states in which a {@link Transaction} call may run; `send` covers `sendBatch` and `sendOffsets` too. */
const LEGAL_STATES: Record<TransactionCall, readonly TransactionState[]> = {
  send: [TRANSACTION_STATES.TRANSACTING],
  commit: [TRANSACTION_STATES.TRANSACTING, TRANSACTION_STATES.COMMITTING],
  abort: [TRANSACTION_STATES.TRANSACTING, TRANSACTION_STATES.ABORTABLE, TRANSACTION_STATES.ABORTING],
};

/** One begun transaction, so an ended one cannot act on the transaction begun after it. */
interface TransactionHandle {
  readonly sender: TransactionSender;
}

/** One transaction's message producer, built by the caller of {@link TransactionSession.begin}. */
export interface TransactionSender {
  send: (record: ProducerRecord & { signal?: AbortSignal }) => Promise<RecordMetadata[]>;
  sendBatch: (batch: ProducerBatch & { signal?: AbortSignal }) => Promise<RecordMetadata[]>;
  flush: () => Promise<void>;
}

/**
 * A transactional producer's lifecycle: begin, the usability of each transaction, the partitions
 * and offsets it holds, flush-then-end, and the epoch bump once it ends. One transaction is open at
 * a time; each send reports to it through the {@link OpenTransaction} methods.
 */
export interface TransactionSession extends OpenTransaction {
  /** Producer id, epoch and sequence numbers for the transactional id; an epoch bump waits until the transaction ends. */
  readonly ledger: IdempotenceLedger;
  /**
   * Begins a transaction once the epoch bump after the previous transaction settles, sending
   * InitProducerId first on the first call and running any requested epoch bump. `createSender`
   * builds the transaction's message producer. Rejects while another transaction is open or once
   * the producer is fenced.
   */
  begin: (createSender: () => TransactionSender) => Promise<Transaction>;
  /** Sends what the open transaction's message producer still buffers. */
  flush: () => Promise<void>;
}

export interface TransactionSessionOptions {
  logger: Logger;
  cluster: Cluster;
  transactionalId: string;
  /** Milliseconds the coordinator waits for the transaction before aborting it. */
  transactionTimeout: number;
  retry: Retrier;
}

export function createTransactionSession({
  logger,
  cluster,
  transactionalId,
  transactionTimeout,
  retry,
}: TransactionSessionOptions): TransactionSession {
  /** Topic-partitions already known to be participating in the current transaction. */
  let transactionTopicPartitions = new Map<string, Set<number>>();

  /** Whether consumer-group offsets have been added to the current transaction. */
  let hasOffsetsAddedToTransaction = false;

  /**
   * Offsets sent with the current transaction. They are marked committed in the cluster's record
   * only once the transaction commits, so an aborted one leaves no offset the group never stored.
   */
  let offsetsToMarkCommitted: Parameters<Cluster['markOffsetAsCommitted']>[0][] = [];

  /** The transaction from `begin` until it ends. */
  let open: TransactionHandle | undefined;

  /** The epoch bump that runs once a transaction ended; the next transaction begins only after it. */
  let postTransactionBump: Promise<void> | undefined;

  const stateMachine = new TransactionStateMachine({ logger });
  stateMachine.on('transition', ({ to }: TransitionEvent) => {
    if (to === TRANSACTION_STATES.READY) {
      transactionTopicPartitions = new Map();
      hasOffsetsAddedToTransaction = false;
      offsetsToMarkCommitted = [];
      open = undefined;
    }
  });

  const ledger = createIdempotenceLedger({
    logger,
    initProducerId: async (current) => {
      const identity = await sendInitProducerId(
        { cluster, retry },
        { transactionalId, transactionTimeout, ...current },
      );
      stateMachine.transitionTo(TRANSACTION_STATES.READY);
      return identity;
    },
    canBumpEpoch: () => stateMachine.state() === TRANSACTION_STATES.READY,
  });

  function findTransactionCoordinator(): Promise<Broker> {
    return cluster.findGroupCoordinator({ groupId: transactionalId, coordinatorType: COORDINATOR_TYPES.TRANSACTION });
  }

  /** KIP-890 part 2: Produce v12+ (Kafka 4.0+) covers AddPartitionsToTxn and bumps the epoch every transaction. */
  function isTransactionV2Enabled(): boolean {
    const versions = cluster.brokerPool.versions;
    return versions != null && supportsTransactionV2(versions);
  }

  /** Whether the transaction holds partitions or offsets, so ending it needs EndTxn. */
  function isOngoing(): boolean {
    return hasOffsetsAddedToTransaction || transactionTopicPartitions.size > 0;
  }

  /**
   * The one answer to whether a transaction may run `call` now: not once the producer is fenced,
   * not once the transaction ended, and only in the states `call` is legal in. Returns why not.
   */
  function unusableError(handle: TransactionHandle, call: TransactionCall): KafkaNonRetriableError | undefined {
    const state = stateMachine.state();
    if (state === TRANSACTION_STATES.FATAL_ERROR) return new KafkaNonRetriableError(FENCED_PRODUCER_MESSAGE);
    if (open !== handle) return new KafkaNonRetriableError('Cannot continue to use transaction once ended');
    if (LEGAL_STATES[call].includes(state)) return undefined;
    if (call === 'send') {
      return new KafkaNonRetriableError(
        'Cannot send or add offsets once the transaction is ending or can only be aborted',
      );
    }
    return stateMachine.illegalCallError(call);
  }

  /** Runs `fn` only when {@link unusableError} allows `call`, rejecting otherwise. */
  function guard<Args extends unknown[], R>(
    handle: TransactionHandle,
    call: TransactionCall,
    fn: (...args: Args) => Promise<R>,
  ): (...args: Args) => Promise<R> {
    return (...args: Args) => {
      const error = unusableError(handle, call);
      if (error) return Promise.reject(error);
      return fn(...args);
    };
  }

  function assertCanBegin(): void {
    const state = stateMachine.state();
    if (state === TRANSACTION_STATES.FATAL_ERROR) throw new KafkaNonRetriableError(FENCED_PRODUCER_MESSAGE);
    if (state !== TRANSACTION_STATES.UNINITIALIZED && state !== TRANSACTION_STATES.READY) {
      throw new KafkaNonRetriableError(
        'There is already an ongoing transaction for this producer. Please end the transaction before beginning another.',
      );
    }
  }

  async function begin(createSender: () => TransactionSender): Promise<Transaction> {
    // The ended transaction is already READY while its bump runs; beginning under the old epoch
    // would race the bump and lose it.
    if (postTransactionBump) await postTransactionBump;
    assertCanBegin();

    // The producer id is initialized once; later transactions only bump the epoch when requested.
    if (!ledger.isInitialized()) await ledger.initProducerId();
    await ledger.bumpEpochIfRequested();
    // A concurrent `begin` may have opened its transaction while this one waited.
    assertCanBegin();

    const handle: TransactionHandle = { sender: createSender() };
    stateMachine.transitionTo(TRANSACTION_STATES.TRANSACTING);
    open = handle;

    return {
      send: guard(handle, 'send', handle.sender.send),
      sendBatch: guard(handle, 'send', handle.sender.sendBatch),
      sendOffsets: guard(handle, 'send', sendOffsets),
      commit: guard(handle, 'commit', () => flushThenEnd(handle, 'commit')),
      abort: guard(handle, 'abort', () => flushThenEnd(handle, 'abort')),
      isActive: () => open === handle && stateMachine.state() === TRANSACTION_STATES.TRANSACTING,
    };
  }

  function enrollPartitions(topicData: readonly TransactionTopicPartitions[]): Promise<void> | undefined {
    if (stateMachine.state() !== TRANSACTION_STATES.TRANSACTING) {
      return Promise.reject(stateMachine.illegalCallError('enrollPartitions'));
    }

    let newTopicPartitions: Map<string, number[]> | undefined;
    for (const { topic, partitions } of topicData) {
      const enrolled = transactionTopicPartitions.get(topic);
      for (const { partition } of partitions) {
        if (enrolled?.has(partition)) continue;
        newTopicPartitions ??= new Map();
        let pending = newTopicPartitions.get(topic);
        if (!pending) {
          pending = [];
          newTopicPartitions.set(topic, pending);
        }
        pending.push(partition);
      }
    }

    if (!newTopicPartitions) return undefined;
    const topics = [...newTopicPartitions].map(([topic, partitions]) => ({ topic, partitions }));

    // KIP-890 part 2: transaction V2 lets Produce itself cover AddPartitionsToTxn, so the
    // partitions are enrolled without the extra round trip to the transaction coordinator.
    if (isTransactionV2Enabled()) {
      markEnrolled(topics);
      return undefined;
    }
    return addPartitionsToTxn(topics);
  }

  async function addPartitionsToTxn(topics: { topic: string; partitions: number[] }[]): Promise<void> {
    const broker = await findTransactionCoordinator();
    await broker.send(AddPartitionsToTxn, {
      transactionalId,
      producerId: ledger.getProducerId(),
      producerEpoch: ledger.getProducerEpoch(),
      topics,
    });
    markEnrolled(topics);
  }

  function markEnrolled(topics: readonly { topic: string; partitions: number[] }[]): void {
    for (const { topic, partitions } of topics) {
      let enrolled = transactionTopicPartitions.get(topic);
      if (!enrolled) {
        enrolled = new Set();
        transactionTopicPartitions.set(topic, enrolled);
      }
      for (const partition of partitions) enrolled.add(partition);
    }
  }

  /**
   * Like the Java client's abortable error state: some partitions may already be written. No-op
   * outside TRANSACTING, so a race with an in-flight commit or abort cannot throw.
   */
  function sendFailed(): void {
    if (stateMachine.state() === TRANSACTION_STATES.TRANSACTING) {
      stateMachine.transitionTo(TRANSACTION_STATES.ABORTABLE);
    }
  }

  /**
   * Sends the transaction's buffered records so they are part of it, then ends it. A send that fails
   * meanwhile can leave it abort-only, so the call is checked again after the flush.
   */
  async function flushThenEnd(handle: TransactionHandle, call: 'commit' | 'abort'): Promise<void> {
    await handle.sender.flush();
    const error = unusableError(handle, call);
    if (error) throw error;
    await endTransaction(call);
  }

  async function endTransaction(call: 'commit' | 'abort'): Promise<void> {
    const transactionResult = call === 'commit';
    const nextState = transactionResult ? TRANSACTION_STATES.COMMITTING : TRANSACTION_STATES.ABORTING;
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
        await broker.send(EndTxn, {
          producerId: ledger.getProducerId(),
          producerEpoch: ledger.getProducerEpoch(),
          transactionalId,
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
        // A newer producer took over this transactionalId (Java treats it as fatal).
        if (isProducerFenced(error)) {
          logger.error('Producer was fenced by a newer instance; it cannot be used anymore', { transactionalId });
          stateMachine.transitionTo(TRANSACTION_STATES.FATAL_ERROR);
        } else if (transactionResult) {
          stateMachine.transitionTo(TRANSACTION_STATES.ABORTABLE);
        }
      }
      throw error;
    });
    if (transactionResult) {
      for (const committedOffset of offsetsToMarkCommitted) cluster.markOffsetAsCommitted(committedOffset);
    }
    stateMachine.transitionTo(TRANSACTION_STATES.READY);

    // KIP-890 part 2: transaction V2 bumps the producer epoch on every transaction (not just on
    // fencing recovery), so the next transaction starts from a fresh epoch.
    if (isTransactionV2Enabled() || ledger.isEpochBumpRequested()) {
      postTransactionBump = bumpEpochAfterTransaction().finally(() => {
        postTransactionBump = undefined;
      });
      await postTransactionBump;
    }
  }

  /**
   * Never rejects: the transaction has already ended, so a failed bump only requests one, which the
   * next `begin` runs.
   */
  async function bumpEpochAfterTransaction(): Promise<void> {
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

  /** Adds consumer-group offsets to the transaction; they are marked committed locally once it commits. */
  async function sendOffsets({
    consumerGroupId,
    topics,
  }: {
    consumerGroupId: string;
    topics: readonly TopicOffsets[];
  }): Promise<void> {
    // Kept from the start, so a call still running when its transaction ends cannot add to the next one.
    const transactionOffsets = offsetsToMarkCommitted;
    const transactionCoordinator = await findTransactionCoordinator();

    await transactionCoordinator.send(AddOffsetsToTxn, {
      transactionalId,
      producerId: ledger.getProducerId(),
      producerEpoch: ledger.getProducerEpoch(),
      groupId: consumerGroupId,
    });

    hasOffsetsAddedToTransaction = true;

    const requestTopics = topics.map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.map(({ partition, offset }) => ({ partition, offset, metadata: null })),
    }));

    await cluster.run(
      coordinatorTarget(consumerGroupId, COORDINATOR_TYPES.GROUP),
      (groupCoordinator) =>
        groupCoordinator.send(TxnOffsetCommit, {
          transactionalId,
          producerId: ledger.getProducerId(),
          producerEpoch: ledger.getProducerEpoch(),
          groupId: consumerGroupId,
          topics: requestTopics,
        }),
      { retrier: retry, retryOn: TXN_OFFSET_COMMIT_RETRY_ON },
    );

    for (const { topic, partitions } of topics) {
      for (const { partition, offset } of partitions) {
        transactionOffsets.push({ groupId: consumerGroupId, topic, partition, offset });
      }
    }
  }

  return {
    transactionalId,
    ledger,
    begin,
    enrollPartitions,
    sendFailed,
    flush: async () => {
      if (open) await open.sender.flush();
    },
  };
}
