import type { Cluster, TopicOffsets } from '../cluster/index.js';
import { KafkaJSNonRetriableError } from '../errors.js';
import { InstrumentationEventEmitter, type RemoveInstrumentationEventListener } from '../instrumentation/emitter.js';
import type { InstrumentationEvent } from '../instrumentation/event.js';
import type { Logger } from '../loggers/index.js';
import { CONNECTION_STATUS, type ConnectionStatus } from '../network/connection-status.js';
import { retrier, type RetryOptions } from '../retry/index.js';
import { abortError, rejectOnAbort, type ConnectOptions } from '../utils/abort.js';
import { createEosManager, type EosManager } from './eos-manager/index.js';
import { CONNECT, DISCONNECT, events, unwrap, wrap, type ProducerEventName } from './instrumentation-events.js';
import { createMessageProducer } from './message-producer.js';
import { DefaultPartitioner } from './partitioners/index.js';
import type { CustomPartitioner, ProducerBatch, ProducerRecord, RecordMetadata } from './types.js';

export interface ProducerOptions {
  cluster: Cluster;
  logger: Logger;
  createPartitioner?: CustomPartitioner;
  retry?: RetryOptions;
  idempotent?: boolean;
  transactionalId?: string;
  transactionTimeout?: number;
  instrumentationEmitter?: InstrumentationEventEmitter | null;
}

export interface Transaction {
  send(record: ProducerRecord & { signal?: AbortSignal }): Promise<RecordMetadata[]>;
  sendBatch(batch: ProducerBatch & { signal?: AbortSignal }): Promise<RecordMetadata[]>;
  sendOffsets(options: { consumerGroupId: string; topics: readonly TopicOffsets[] }): Promise<void>;
  commit(): Promise<void>;
  abort(): Promise<void>;
  isActive(): boolean;
}

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
  transaction: () => Promise<Transaction>;
  logger: () => Logger;
  [Symbol.asyncDispose]: () => Promise<void>;
}

const EVENT_NAMES: ReadonlySet<string> = new Set(Object.values(events));
const EVENT_KEYS = Object.keys(events)
  .map((key) => `producer.events.${key}`)
  .join(', ');

/**
 * The user-facing producer: message sending (via `message-producer.ts`), transactions (via
 * `eos-manager/`), and instrumentation events. Mirrors kafkajs's `producer/index.js`.
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
}: ProducerOptions): Producer {
  let connectionStatus: ConnectionStatus = CONNECTION_STATUS.DISCONNECTED;
  const producerRetry: RetryOptions = retry ?? { retries: idempotent ? Number.MAX_SAFE_INTEGER : 5 };

  if (idempotent && producerRetry.retries !== undefined && producerRetry.retries < 1) {
    throw new KafkaJSNonRetriableError('Idempotent producer must allow retries to protect against transient errors');
  }

  const logger = rootLogger.namespace('Producer');

  if (idempotent && producerRetry.retries !== undefined && producerRetry.retries < Number.MAX_SAFE_INTEGER) {
    logger.warn('Limiting retries for the idempotent producer may invalidate EoS guarantees');
  }

  const partitioner = createPartitioner();
  const producerRetrier = retrier(producerRetry);
  const instrumentationEmitter = rootInstrumentationEmitter ?? new InstrumentationEventEmitter();
  const idempotentEosManager = createEosManager({
    logger,
    cluster,
    transactionTimeout,
    transactional: false,
    transactionalId,
  });

  const { send, sendBatch } = createMessageProducer({
    logger,
    cluster,
    partitioner,
    eosManager: idempotentEosManager,
    idempotent,
    retrier: producerRetrier,
    getConnectionStatus: () => connectionStatus,
  });

  let transactionalEosManager: EosManager | undefined;

  function on(
    eventName: ProducerEventName,
    listener: (event: InstrumentationEvent<unknown>) => void | Promise<void>,
  ): RemoveInstrumentationEventListener {
    if (!EVENT_NAMES.has(eventName)) {
      throw new KafkaJSNonRetriableError(`Event name should be one of ${EVENT_KEYS}`);
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
      throw new KafkaJSNonRetriableError('Must provide transactional id for transactional producer');
    }

    let transactionDidEnd = false;
    transactionalEosManager ??= createEosManager({
      logger,
      cluster,
      transactionTimeout,
      transactional: true,
      transactionalId,
    });
    const activeEosManager = transactionalEosManager;

    if (activeEosManager.isInTransaction()) {
      throw new KafkaJSNonRetriableError(
        'There is already an ongoing transaction for this producer. Please end the transaction before beginning another.',
      );
    }

    // We only initialize the producer id once.
    if (!activeEosManager.isInitialized()) {
      await activeEosManager.initProducerId();
    }
    activeEosManager.beginTransaction();

    const { send: sendTxn, sendBatch: sendBatchTxn } = createMessageProducer({
      logger,
      cluster,
      partitioner,
      retrier: producerRetrier,
      eosManager: activeEosManager,
      idempotent: true,
      getConnectionStatus: () => connectionStatus,
    });

    const isActive = (): boolean => activeEosManager.isInTransaction() && !transactionDidEnd;

    function transactionGuard<Args extends unknown[], R>(
      fn: (...args: Args) => Promise<R>,
    ): (...args: Args) => Promise<R> {
      return (...args: Args) => {
        if (!isActive()) {
          return Promise.reject(new KafkaJSNonRetriableError('Cannot continue to use transaction once ended'));
        }

        return fn(...args);
      };
    }

    return {
      sendBatch: transactionGuard(sendBatchTxn),
      send: transactionGuard(sendTxn),
      abort: transactionGuard(async () => {
        await activeEosManager.abort();
        transactionDidEnd = true;
      }),
      commit: transactionGuard(async () => {
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

  async function connect({ signal }: ConnectOptions = {}): Promise<void> {
    if (signal?.aborted) throw abortError(signal);
    await rejectOnAbort(cluster.connect(), signal);
    connectionStatus = CONNECTION_STATUS.CONNECTED;
    instrumentationEmitter.emit(CONNECT, {});

    if (idempotent && !idempotentEosManager.isInitialized()) {
      await rejectOnAbort(idempotentEosManager.initProducerId(), signal);
    }
  }

  async function disconnect({ signal }: ConnectOptions = {}): Promise<void> {
    if (signal?.aborted) throw abortError(signal);
    connectionStatus = CONNECTION_STATUS.DISCONNECTING;
    await rejectOnAbort(cluster.disconnect(), signal);
    connectionStatus = CONNECTION_STATUS.DISCONNECTED;
    instrumentationEmitter.emit(DISCONNECT, {});
  }

  return {
    connect,
    disconnect,
    isIdempotent: () => idempotent,
    events,
    on,
    send,
    sendBatch,
    transaction,
    logger: () => logger,
    [Symbol.asyncDispose]: disconnect,
  };
}
