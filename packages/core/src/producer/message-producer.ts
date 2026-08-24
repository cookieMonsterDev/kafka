import { supportsHeaders, supportsZstd } from '../broker/capabilities';
import type { Cluster } from '../cluster/index';
import { KafkaDeliveryTimeoutError, KafkaError, KafkaNonRetriableError, KafkaTimeout } from '../errors';
import type { Logger } from '../loggers/index';
import { CONNECTION_STATUS, type ConnectionStatus } from '../network/connection-status';
import { COMPRESSION_TYPES, type CompressionType } from '../protocol/compression/index';
import type { Retrier } from '../retry/index';
import { rejectOnAbort } from '../utils/abort';
import type { EosManager } from './eos-manager/index';
import { createNodeLatencyTracker, type NodeLatencyTracker } from './node-latency-tracker';
import { createSendMessages } from './send-messages';
import type { CustomPartitioner, Message, ProducerBatch, ProducerRecord, RecordMetadata, TopicMessages } from './types';

export interface MessageProducerOptions {
  logger: Logger;
  cluster: Cluster;
  partitioner: ReturnType<CustomPartitioner>;
  eosManager: EosManager;
  idempotent: boolean;
  retrier: Retrier;
  /** Shared across a producer's lifetime, including transactions. Defaults to a fresh tracker. */
  nodeLatencyTracker?: NodeLatencyTracker;
  getConnectionStatus: () => ConnectionStatus;
  /** Used when send/sendBatch omit acks. Falls back to -1 (all ISR). */
  defaultAcks?: number;
  /** Used when send/sendBatch omit compression. Omit for none. */
  defaultCompression?: CompressionType;
  /** Used when send/sendBatch omit compressionLevel. @see ProducerRecord.compressionLevel */
  defaultCompressionLevel?: number;
  /**
   * Delay in ms to wait for more records before sending a Produce request.
   * Default 0 (send immediately). Java 4.0+ defaults to 5.
   * @see https://kafka.apache.org/43/configuration/producer-configs/#linger.ms
   */
  lingerMs?: number;
  /**
   * Soft cap on buffered record bytes before a Produce is sent (with lingerMs).
   * Ignored when lingerMs is 0. Unset or 0 means do not batch by size.
   * @see https://kafka.apache.org/43/configuration/producer-configs/#batch.size
   */
  batchSize?: number;
  /**
   * Max bytes of linger-buffered records. `send()` waits until a flush frees space,
   * or rejects with `KafkaTimeout` after the send timeout. Unset or 0 is unlimited.
   * Ignored when lingerMs is 0. @see https://kafka.apache.org/43/configuration/producer-configs/#buffer.memory
   */
  bufferMemory?: number;
  /**
   * End-to-end deadline for one send/sendBatch call: linger wait, buffer-memory wait, and every
   * retry attempt, together. Once it elapses, the call rejects with `KafkaDeliveryTimeoutError`
   * regardless of retries remaining. Default 120_000; 0 (or below) disables the deadline.
   * The already in-flight attempt, if any, is not cancelled - same as `signal` abort.
   * @see https://kafka.apache.org/43/configuration/producer-configs/#delivery.timeout.ms
   */
  deliveryTimeoutMs?: number;
}

export interface MessageProducer {
  send: (record: ProducerRecord & { signal?: AbortSignal }) => Promise<RecordMetadata[]>;
  sendBatch: (batch: ProducerBatch & { signal?: AbortSignal }) => Promise<RecordMetadata[]>;
  /** Send any linger-buffered records immediately. No-op when the buffer is empty. */
  flush: () => Promise<void>;
}

const DEFAULT_ACKS = -1;
const DEFAULT_TIMEOUT = 30_000;
const DEFAULT_LINGER_MS = 0;
const DEFAULT_DELIVERY_TIMEOUT_MS = 120_000;

/**
 * Races `promise` against `deliveryTimeoutMs`. Doesn't cancel the underlying send - the retrier
 * has no abort hook - but the caller sees the rejection as soon as the deadline is up instead of
 * waiting out however many retries are left, same "reject, don't cancel" contract as `rejectOnAbort`.
 */
function rejectOnDeliveryTimeout<T>(promise: Promise<T>, deliveryTimeoutMs: number): Promise<T> {
  if (!(deliveryTimeoutMs > 0)) return promise;

  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new KafkaDeliveryTimeoutError(deliveryTimeoutMs)), deliveryTimeoutMs);
    promise.then(resolve, reject).finally(() => clearTimeout(timer));
  });
}

interface PendingSend {
  topicMessages: TopicMessages[];
  topics: Set<string>;
  acks: number;
  compression: CompressionType | undefined;
  compressionLevel: number | undefined;
  timeout: number;
  resolve: (metadata: RecordMetadata[]) => void;
  reject: (error: unknown) => void;
  bytes: number;
}

function fieldBytes(value: Buffer | string | null | undefined): number {
  if (value == null) return 0;
  return Buffer.byteLength(value);
}

function messageBytes(message: Message): number {
  return fieldBytes(message.key) + fieldBytes(message.value);
}

function topicMessagesBytes(topicMessages: readonly TopicMessages[]): number {
  let total = 0;
  for (const { messages } of topicMessages) {
    for (const message of messages) {
      total += messageBytes(message);
    }
  }
  return total;
}

function mergeTopicMessages(entries: readonly PendingSend[]): TopicMessages[] {
  const mergedByTopic = new Map<string, Message[]>();
  for (const { topicMessages } of entries) {
    for (const { topic, messages } of topicMessages) {
      const current = mergedByTopic.get(topic);
      if (current) {
        current.push(...messages);
      } else {
        mergedByTopic.set(topic, [...messages]);
      }
    }
  }

  return [...mergedByTopic.entries()].map(([topic, messages]) => ({ topic, messages }));
}

function groupKey(entry: PendingSend): string {
  const compression = entry.compression ?? COMPRESSION_TYPES.None;
  const compressionLevel = entry.compressionLevel ?? '';
  return `${entry.acks}\0${compression}\0${compressionLevel}\0${entry.timeout}`;
}

interface BufferWaiter {
  bytes: number;
  resolve: () => void;
  reject: (error: Error) => void;
}

export function createMessageProducer({
  logger,
  cluster,
  partitioner,
  eosManager,
  idempotent,
  retrier,
  nodeLatencyTracker = createNodeLatencyTracker(),
  getConnectionStatus,
  defaultAcks,
  defaultCompression,
  defaultCompressionLevel,
  lingerMs = DEFAULT_LINGER_MS,
  batchSize = 0,
  bufferMemory,
  deliveryTimeoutMs = DEFAULT_DELIVERY_TIMEOUT_MS,
}: MessageProducerOptions): MessageProducer {
  const sendMessages = createSendMessages({ logger, cluster, retrier, partitioner, eosManager, nodeLatencyTracker });
  const pending: PendingSend[] = [];
  let lingerTimer: ReturnType<typeof setTimeout> | null = null;
  let flushInProgress: Promise<void> | null = null;
  let bufferedBytes = 0;
  const bufferWaiters: BufferWaiter[] = [];

  function validateConnectionStatus(): void {
    const connectionStatus = getConnectionStatus();

    if (connectionStatus === CONNECTION_STATUS.DISCONNECTING) {
      throw new KafkaNonRetriableError(
        `The producer is disconnecting; therefore, it can't safely accept messages anymore`,
      );
    }

    if (connectionStatus === CONNECTION_STATUS.DISCONNECTED) {
      throw new KafkaError('The producer is disconnected');
    }
  }

  function validateBatch(
    topicMessages: readonly TopicMessages[],
    acks: number,
    compression: CompressionType | undefined,
  ): void {
    if (topicMessages.some(({ topic }) => !topic)) {
      throw new KafkaNonRetriableError('Invalid topic');
    }

    if (idempotent && acks !== -1) {
      throw new KafkaNonRetriableError(
        `Not requiring ack for all messages invalidates the idempotent producer's EoS guarantees`,
      );
    }

    for (const { topic, messages } of topicMessages) {
      if (!messages) {
        throw new KafkaNonRetriableError(`Invalid messages array [${String(messages)}] for topic "${topic}"`);
      }

      const messageWithoutValue = messages.find((message) => message.value === undefined);
      if (messageWithoutValue) {
        throw new KafkaNonRetriableError(
          `Invalid message without value for topic "${topic}": ${JSON.stringify(messageWithoutValue)}`,
        );
      }
    }

    validateConnectionStatus();

    const versions = cluster.brokerPool.versions;
    const hasHeaders = topicMessages.some(({ messages }) =>
      messages.some((message) => message.headers != null && Object.keys(message.headers).length > 0),
    );
    if (hasHeaders && (versions == null || !supportsHeaders(versions))) {
      throw new KafkaNonRetriableError('Message headers require Produce API version 3 or higher (Kafka 0.11+)');
    }

    if (compression === COMPRESSION_TYPES.ZSTD) {
      if (versions == null || !supportsZstd(versions)) {
        throw new KafkaNonRetriableError('ZSTD compression requires Produce API version 7 or higher (Kafka 2.1+)');
      }
    }
  }

  function mergeCallTopicMessages(topicMessages: readonly TopicMessages[]): TopicMessages[] {
    const mergedByTopic = new Map<string, Message[]>();
    for (const { topic, messages } of topicMessages) {
      const current = mergedByTopic.get(topic);
      if (current) {
        current.push(...messages);
      } else {
        mergedByTopic.set(topic, [...messages]);
      }
    }

    return [...mergedByTopic.entries()].map(([topic, messages]) => ({ topic, messages }));
  }

  function dispatch(
    topicMessages: readonly TopicMessages[],
    acks: number,
    timeout: number,
    compression: CompressionType | undefined,
    compressionLevel: number | undefined,
  ): Promise<RecordMetadata[]> {
    return sendMessages({ acks, timeout, compression, compressionLevel, topicMessages });
  }

  function hasBufferLimit(): boolean {
    return lingerMs > 0 && bufferMemory != null && bufferMemory > 0;
  }

  function canAdmit(bytes: number): boolean {
    return !hasBufferLimit() || bufferedBytes + bytes <= (bufferMemory ?? 0);
  }

  function notifyBufferWaiters(): void {
    while (bufferWaiters[0] && canAdmit(bufferWaiters[0].bytes)) {
      const waiter = bufferWaiters.shift();
      if (!waiter) break;
      bufferedBytes += waiter.bytes;
      waiter.resolve();
    }
  }

  function releaseBufferedBytes(bytes: number): void {
    bufferedBytes -= bytes;
    if (bufferedBytes < 0) bufferedBytes = 0;
    notifyBufferWaiters();
  }

  async function reserveBuffer(bytes: number, timeout: number): Promise<void> {
    if (hasBufferLimit() && bytes > (bufferMemory ?? 0)) {
      throw new KafkaNonRetriableError(`Record batch of ${bytes} bytes exceeds bufferMemory (${bufferMemory} bytes)`);
    }

    if (!hasBufferLimit() || canAdmit(bytes)) {
      bufferedBytes += bytes;
      return;
    }

    await new Promise<void>((resolve, reject) => {
      const waiter: BufferWaiter = {
        bytes,
        resolve: () => {
          clearTimeout(timer);
          resolve();
        },
        reject: (error: Error) => {
          clearTimeout(timer);
          reject(error);
        },
      };
      const timer = setTimeout(() => {
        const index = bufferWaiters.indexOf(waiter);
        if (index >= 0) bufferWaiters.splice(index, 1);
        waiter.reject(new KafkaTimeout(`Timeout while waiting for producer bufferMemory (${bufferMemory} bytes)`));
      }, timeout);
      bufferWaiters.push(waiter);
      void startFlush();
    });
  }

  function clearLingerTimer(): void {
    if (lingerTimer != null) {
      clearTimeout(lingerTimer);
      lingerTimer = null;
    }
  }

  function scheduleLinger(): void {
    if (lingerTimer != null || lingerMs <= 0) return;
    lingerTimer = setTimeout(() => {
      lingerTimer = null;
      void startFlush();
    }, lingerMs);
  }

  function shouldFlushBySize(): boolean {
    return lingerMs > 0 && batchSize > 0 && bufferedBytes >= batchSize;
  }

  async function sendGrouped(entries: readonly PendingSend[]): Promise<void> {
    const groups = new Map<string, PendingSend[]>();
    for (const entry of entries) {
      const key = groupKey(entry);
      const group = groups.get(key);
      if (group) {
        group.push(entry);
      } else {
        groups.set(key, [entry]);
      }
    }

    await Promise.all(
      [...groups.values()].map(async (group) => {
        const first = group[0];
        if (!first) return;

        try {
          const metadata = await dispatch(
            mergeTopicMessages(group),
            first.acks,
            first.timeout,
            first.compression,
            first.compressionLevel,
          );
          for (const entry of group) {
            entry.resolve(metadata.filter((item) => entry.topics.has(item.topicName)));
          }
        } catch (error) {
          for (const entry of group) {
            entry.reject(error);
          }
        }
      }),
    );
  }

  async function doFlush(): Promise<void> {
    clearLingerTimer();
    const entries = pending.splice(0);
    if (entries.length === 0) return;
    try {
      await sendGrouped(entries);
    } finally {
      let flushedBytes = 0;
      for (const entry of entries) {
        flushedBytes += entry.bytes;
      }
      releaseBufferedBytes(flushedBytes);
    }
  }

  function startFlush(): Promise<void> {
    if (!flushInProgress) {
      flushInProgress = doFlush().finally(() => {
        flushInProgress = null;
        if (pending.length === 0) return;
        if (shouldFlushBySize()) {
          void startFlush();
        } else {
          scheduleLinger();
        }
      });
    }

    return flushInProgress;
  }

  async function flush(): Promise<void> {
    while (pending.length > 0 || flushInProgress) {
      await startFlush();
    }
  }

  async function enqueue(
    topicMessages: TopicMessages[],
    acks: number,
    timeout: number,
    compression: CompressionType | undefined,
    compressionLevel: number | undefined,
  ): Promise<RecordMetadata[]> {
    const bytes = topicMessagesBytes(topicMessages);
    await reserveBuffer(bytes, timeout);

    let resolve!: (metadata: RecordMetadata[]) => void;
    let reject!: (error: unknown) => void;
    const result = new Promise<RecordMetadata[]>((res, rej) => {
      resolve = res;
      reject = rej;
    });

    pending.push({
      topicMessages,
      topics: new Set(topicMessages.map(({ topic }) => topic)),
      acks,
      compression,
      compressionLevel,
      timeout,
      resolve,
      reject,
      bytes,
    });

    if (shouldFlushBySize()) {
      void startFlush();
    } else {
      scheduleLinger();
    }

    return result;
  }

  async function sendBatch({
    acks,
    timeout,
    compression,
    compressionLevel,
    topicMessages = [],
    signal,
  }: ProducerBatch & { signal?: AbortSignal }): Promise<RecordMetadata[]> {
    const resolvedAcks = acks ?? defaultAcks ?? DEFAULT_ACKS;
    const resolvedTimeout = timeout ?? DEFAULT_TIMEOUT;
    const resolvedCompression = compression ?? defaultCompression;
    const resolvedCompressionLevel = compressionLevel ?? defaultCompressionLevel;

    validateBatch(topicMessages, resolvedAcks, resolvedCompression);
    const mergedTopicMessages = mergeCallTopicMessages(topicMessages);

    const produce =
      lingerMs <= 0
        ? dispatch(mergedTopicMessages, resolvedAcks, resolvedTimeout, resolvedCompression, resolvedCompressionLevel)
        : enqueue(mergedTopicMessages, resolvedAcks, resolvedTimeout, resolvedCompression, resolvedCompressionLevel);

    return rejectOnAbort(rejectOnDeliveryTimeout(produce, deliveryTimeoutMs), signal);
  }

  async function send({
    acks,
    timeout,
    compression,
    compressionLevel,
    topic,
    messages,
    signal,
  }: ProducerRecord & { signal?: AbortSignal }): Promise<RecordMetadata[]> {
    return sendBatch({ acks, timeout, compression, compressionLevel, topicMessages: [{ topic, messages }], signal });
  }

  return { send, sendBatch, flush };
}
