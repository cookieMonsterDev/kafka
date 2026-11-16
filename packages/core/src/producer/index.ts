import { supportsTransactions } from '../broker/capabilities';
import type { Cluster, TopicPartitionInfo } from '../cluster/index';
import { sendInitProducerId } from '../cluster/init-producer-id';
import { KafkaNonRetriableError } from '../errors';
import { InstrumentationEventEmitter, type RemoveInstrumentationEventListener } from '../instrumentation/emitter';
import type { InstrumentationEvent } from '../instrumentation/event';
import type { MetricsRecorder } from '../instrumentation/metrics';
import type { Logger } from '../loggers/index';
import { CONNECTION_STATUS, type ConnectionStatus } from '../network/connection-status';
import type { CompressionType } from '../protocol/compression/index';
import { retrier, type RetryOptions } from '../retry/index';
import { abortError, rejectOnAbort, type ConnectOptions } from '../utils/abort';
import { createIdempotenceLedger } from './idempotence-ledger';
import { CONNECT, DISCONNECT, events, unwrap, wrap, type ProducerEventName } from './instrumentation-events';
import { createMessageProducer } from './message-producer';
import { createNodeLatencyTracker } from './node-latency-tracker';
import { DefaultPartitioner } from './partitioners/index';
import { createTransactionSession, type TransactionSession } from './transaction-session/index';
import type {
  CustomPartitioner,
  ProducerBatch,
  ProducerHooks,
  ProducerRecord,
  RecordMetadata,
  Transaction,
} from './types';

export type { Transaction } from './types';

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
  compressionLevel?: number;
  lingerMs?: number;
  batchSize?: number;
  bufferMemory?: number;
  deliveryTimeoutMs?: number;
  maxRequestSize?: number;
  /** Ordered async `onSend`/`onAck` hooks, also used by every {@link Transaction} this producer starts. */
  hooks?: ProducerHooks;
  metrics?: MetricsRecorder | null;
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
  /** Topic names from Metadata (all topics the broker will describe). Connects if needed. */
  listTopics: () => Promise<string[]>;
  /** Partition leaders/replicas/ISR for `topic`. Connects and refreshes metadata if needed. */
  partitionsFor: (topic: string) => Promise<TopicPartitionInfo[]>;
  /** KIP-714 client instance UUID, or `null` until the broker assigns one (or telemetry is off). */
  clientInstanceId: () => Buffer | null;
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
  transactionTimeout = 60_000,
  instrumentationEmitter: rootInstrumentationEmitter,
  acks,
  compression,
  compressionLevel,
  lingerMs = 5,
  batchSize = 16_384,
  bufferMemory,
  deliveryTimeoutMs,
  maxRequestSize,
  hooks,
  metrics,
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
  const idempotenceLedger = createIdempotenceLedger({
    logger,
    initProducerId: (current) =>
      sendInitProducerId(
        { cluster, retry: producerRetrier, logger },
        { transactionalId: null, transactionTimeout, ...current },
      ),
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
    defaultCompressionLevel: compressionLevel,
    lingerMs,
    batchSize,
    bufferMemory,
    deliveryTimeoutMs,
    maxRequestSize,
    hooks,
    metrics,
  };

  const { send, sendBatch, flush } = createMessageProducer({
    ...messageProducerOptions,
    ledger: idempotenceLedger,
    idempotent,
  });

  let transactionSession: TransactionSession | undefined;

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

    transactionSession ??= createTransactionSession({
      logger,
      cluster,
      transactionalId,
      transactionTimeout,
      retry: producerRetrier,
    });
    const session = transactionSession;
    return session.begin(() =>
      createMessageProducer({
        ...messageProducerOptions,
        ledger: session.ledger,
        transaction: session,
        idempotent: true,
      }),
    );
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

    if (idempotent && !idempotenceLedger.isInitialized()) {
      await rejectOnAbort(idempotenceLedger.initProducerId(), signal);
    }
  }

  async function flushAll(): Promise<void> {
    await flush();
    if (transactionSession) await transactionSession.flush();
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
    listTopics: async () => {
      if (connectionStatus !== CONNECTION_STATUS.CONNECTED) {
        await connect();
      }
      return cluster.listTopics();
    },
    partitionsFor: async (topic: string) => {
      if (connectionStatus !== CONNECTION_STATUS.CONNECTED) {
        await connect();
      }
      return cluster.partitionsFor(topic);
    },
    clientInstanceId: () => cluster.clientInstanceId(),
    logger: () => logger,
    [Symbol.asyncDispose]: disconnect,
  };
}
