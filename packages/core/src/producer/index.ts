import { supportsTransactions } from '../broker/capabilities';
import type { Cluster, TopicOffsets } from '../cluster/index';
import { KafkaNonRetriableError } from '../errors';
import { InstrumentationEventEmitter, type RemoveInstrumentationEventListener } from '../instrumentation/emitter';
import type { InstrumentationEvent } from '../instrumentation/event';
import type { Logger } from '../loggers/index';
import { CONNECTION_STATUS, type ConnectionStatus } from '../network/connection-status';
import type { CompressionType } from '../protocol/compression/index';
import { retrier, type RetryOptions } from '../retry/index';
import { abortError, rejectOnAbort, type ConnectOptions } from '../utils/abort';
import { createEosManager, type EosManager } from './eos-manager/index';
import { CONNECT, DISCONNECT, events, unwrap, wrap, type ProducerEventName } from './instrumentation-events';
import { createMessageProducer } from './message-producer';
import { createNodeLatencyTracker } from './node-latency-tracker';
import { DefaultPartitioner } from './partitioners/index';
import type { CustomPartitioner, ProducerBatch, ProducerHooks, ProducerRecord, RecordMetadata } from './types';

export interface ProducerOptions {
  cluster: Cluster;
  logger: Logger;
  createPartitioner?: CustomPartitioner;
  retry?: RetryOptions;
  idempotent?: boolean;
  transactionalId?: string;
  transactionTimeout?: number;
  instrumentationEmitter?: InstrumentationEventEmitter | null;
  acks?: number;
  compression?: CompressionType;
  lingerMs?: number;
  batchSize?: number;
  bufferMemory?: number;
  deliveryTimeoutMs?: number;
  maxRequestSize?: number;
  /** Ordered async `onSend`/`onAck` hooks, also used by every {@link Transaction} this producer starts. */
  hooks?: ProducerHooks;
}

/**
 * In-flight producer transaction. Exactly one may be active per transactional producer.
 * @see https://kafka.apache.org/43/configuration/producer-configs/#transactional.id
 */
export interface Transaction {
  send(record: ProducerRecord & { signal?: AbortSignal }): Promise<RecordMetadata[]>;
  sendBatch(batch: ProducerBatch & { signal?: AbortSignal }): Promise<RecordMetadata[]>;
  /** Commit consumer-group offsets as part of this transaction (exactly-once consume-transform-produce). */
  sendOffsets(options: { consumerGroupId: string; topics: readonly TopicOffsets[] }): Promise<void>;
  commit(): Promise<void>;
  abort(): Promise<void>;
  isActive(): boolean;
}

/**
 * Producer returned by {@link Kafka.producer}.
 * @see https://kafka.apache.org/43/configuration/producer-configs/
 */
export interface Producer {
  connect: (options?: ConnectOptions) => Promise<void>;
  disconnect: (options?: ConnectOptions) => Promise<void>;
  isIdempotent: () => boolean;
  readonly events: typeof events;
  on: (
    eventName: ProducerEventName,
    listener: (event: InstrumentationEvent<unknown>) => void | Promise<void>,
  ) => RemoveInstrumentationEventListener;
  send: (record: ProducerRecord & { signal?: AbortSignal }) => Promise<RecordMetadata[]>;
  sendBatch: (batch: ProducerBatch & { signal?: AbortSignal }) => Promise<RecordMetadata[]>;
  /** Send any linger-buffered records immediately. No-op when lingerMs is 0. */
  flush: () => Promise<void>;
  /** Begin a transaction. Requires `transactionalId` on the producer. */
  transaction: () => Promise<Transaction>;
  logger: () => Logger;
  [Symbol.asyncDispose]: () => Promise<void>;
}

const EVENT_NAMES: ReadonlySet<string> = new Set(Object.values(events));
const EVENT_KEYS = Object.keys(events)
  .map((key) => `producer.events.${key}`)
  .join(', ');

/**
 * User-facing producer: send path, optional transactions, and instrumentation events.
 *
 * @see https://kafka.apache.org/43/configuration/producer-configs/
 */
export function createProducer({
  cluster,
  logger: rootLogger,
  createPartitioner = DefaultPartitioner,
  retry,
  idempotent = false,
  transactionalId,
  transactionTimeout,
  instrumentationEmitter: rootInstrumentationEmitter,
  acks,
  compression,
  lingerMs = 0,
  batchSize,
  bufferMemory,
  deliveryTimeoutMs,
  maxRequestSize,
  hooks,
}: ProducerOptions): Producer {
  let connectionStatus: ConnectionStatus = CONNECTION_STATUS.DISCONNECTED;
  const producerRetry: RetryOptions = retry ?? { retries: idempotent ? Number.MAX_SAFE_INTEGER : 5 };

  if (idempotent && producerRetry.retries !== undefined && producerRetry.retries < 1) {
    throw new KafkaNonRetriableError('Idempotent producer must allow retries to protect against transient errors');
  }

  const logger = rootLogger.namespace('Producer');

  if (idempotent && producerRetry.retries !== undefined && producerRetry.retries < Number.MAX_SAFE_INTEGER) {
    logger.warn('Limiting retries for the idempotent producer may invalidate EoS guarantees');
  }

  const partitioner = createPartitioner();
  const producerRetrier = retrier(producerRetry);
  const instrumentationEmitter = rootInstrumentationEmitter ?? new InstrumentationEventEmitter();
  // Shared across the idempotent producer and every transaction() below - it's about how fast
  // this producer's broker nodes respond, not anything specific to one message-producer instance.
  const nodeLatencyTracker = createNodeLatencyTracker();
  const idempotentEosManager = createEosManager({
    logger,
    cluster,
    transactionTimeout,
    transactional: false,
    transactionalId,
    retry: producerRetry,
  });

  const messageProducerOptions = {
    logger,
    cluster,
    partitioner,
    retrier: producerRetrier,
    nodeLatencyTracker,
    getConnectionStatus: () => connectionStatus,
    defaultAcks: acks,
    defaultCompression: compression,
    lingerMs,
    batchSize,
    bufferMemory,
    deliveryTimeoutMs,
    maxRequestSize,
    hooks,
  };

  const { send, sendBatch, flush } = createMessageProducer({
    ...messageProducerOptions,
    eosManager: idempotentEosManager,
    idempotent,
  });

  let transactionalEosManager: EosManager | undefined;
  let transactionalFlush: (() => Promise<void>) | undefined;

  function on(
    eventName: ProducerEventName,
    listener: (event: InstrumentationEvent<unknown>) => void | Promise<void>,
  ): RemoveInstrumentationEventListener {
    if (!EVENT_NAMES.has(eventName)) {
      throw new KafkaNonRetriableError(`Event name should be one of ${EVENT_KEYS}`);
    }

    return instrumentationEmitter.addListener(unwrap(eventName), (event: InstrumentationEvent<unknown>) => {
      const wrapped = { ...event, type: wrap(event.type) };
      Promise.resolve(listener(wrapped)).catch((e: unknown) => {
        const error = e as Error;
        logger.error(`Failed to execute listener: ${error.message}`, { eventName, stack: error.stack });
      });
    });
  }

  async function transaction(): Promise<Transaction> {
    if (!transactionalId) {
      throw new KafkaNonRetriableError('Must provide transactional id for transactional producer');
    }

    assertBrokerSupportsTransactions();

    let transactionDidEnd = false;
    transactionalEosManager ??= createEosManager({
      logger,
      cluster,
      transactionTimeout,
      transactional: true,
      transactionalId,
      retry: producerRetry,
    });
    const activeEosManager = transactionalEosManager;

    if (activeEosManager.isInTransaction()) {
      throw new KafkaNonRetriableError(
        'There is already an ongoing transaction for this producer. Please end the transaction before beginning another.',
      );
    }

    // We only initialize the producer id once.
    if (!activeEosManager.isInitialized()) {
      await activeEosManager.initProducerId();
    }
    activeEosManager.beginTransaction();

    const {
      send: sendTxn,
      sendBatch: sendBatchTxn,
      flush: flushTxn,
    } = createMessageProducer({
      ...messageProducerOptions,
      eosManager: activeEosManager,
      idempotent: true,
    });
    transactionalFlush = flushTxn;

    const isActive = (): boolean => activeEosManager.isInTransaction() && !transactionDidEnd;

    function transactionGuard<Args extends unknown[], R>(
      fn: (...args: Args) => Promise<R>,
    ): (...args: Args) => Promise<R> {
      return (...args: Args) => {
        if (!isActive()) {
          return Promise.reject(new KafkaNonRetriableError('Cannot continue to use transaction once ended'));
        }

        return fn(...args);
      };
    }

    return {
      sendBatch: transactionGuard(sendBatchTxn),
      send: transactionGuard(sendTxn),
      abort: transactionGuard(async () => {
        await flushTxn();
        await activeEosManager.abort();
        transactionDidEnd = true;
      }),
      commit: transactionGuard(async () => {
        await flushTxn();
        await activeEosManager.commit();
        transactionDidEnd = true;
      }),
      sendOffsets: transactionGuard(
        async ({ consumerGroupId, topics }: { consumerGroupId: string; topics: readonly TopicOffsets[] }) => {
          await activeEosManager.sendOffsets({ consumerGroupId, topics });

          for (const { topic, partitions } of topics) {
            for (const { partition, offset } of partitions) {
              cluster.markOffsetAsCommitted({ groupId: consumerGroupId, topic, partition, offset });
            }
          }
        },
      ),
      isActive,
    };
  }

  function assertBrokerSupportsTransactions(): void {
    const versions = cluster.brokerPool.versions;
    if (versions == null || !supportsTransactions(versions)) {
      throw new KafkaNonRetriableError('Idempotent and transactional producers require InitProducerId (Kafka 0.11+)');
    }
  }

  async function connect({ signal }: ConnectOptions = {}): Promise<void> {
    if (signal?.aborted) throw abortError(signal);
    await rejectOnAbort(cluster.connect(), signal);
    connectionStatus = CONNECTION_STATUS.CONNECTED;
    instrumentationEmitter.emit(CONNECT, {});

    if (idempotent || transactionalId) {
      assertBrokerSupportsTransactions();
    }

    if (idempotent && !idempotentEosManager.isInitialized()) {
      await rejectOnAbort(idempotentEosManager.initProducerId(), signal);
    }
  }

  async function flushAll(): Promise<void> {
    await flush();
    if (transactionalFlush) await transactionalFlush();
  }

  async function disconnect({ signal }: ConnectOptions = {}): Promise<void> {
    if (signal?.aborted) throw abortError(signal);
    try {
      await rejectOnAbort(flushAll(), signal);
    } finally {
      connectionStatus = CONNECTION_STATUS.DISCONNECTING;
      await rejectOnAbort(cluster.disconnect(), signal);
      connectionStatus = CONNECTION_STATUS.DISCONNECTED;
      instrumentationEmitter.emit(DISCONNECT, {});
    }
  }

  return {
    connect,
    disconnect,
    isIdempotent: () => idempotent,
    events,
    on,
    send,
    sendBatch,
    flush: flushAll,
    transaction,
    logger: () => logger,
    [Symbol.asyncDispose]: disconnect,
  };
}
