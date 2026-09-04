import { supportsHeaders, supportsZstd } from '../broker/capabilities';
import type { Cluster } from '../cluster/index';
import {
  KafkaDeliveryTimeoutError,
  KafkaError,
  KafkaMessageTooLargeError,
  KafkaNonRetriableError,
  KafkaTimeout,
} from '../errors';
import type { MetricsRecorder } from '../instrumentation/metrics';
import type { Logger } from '../loggers/index';
import { CONNECTION_STATUS, type ConnectionStatus } from '../network/connection-status';
import { COMPRESSION_TYPES, type CompressionType } from '../protocol/compression/index';
import type { Retrier } from '../retry/index';
import { rejectOnAbort } from '../utils/abort';
import { runHooks } from '../utils/run-hooks';
import type { EosManager } from './eos-manager/index';
import { createNodeLatencyTracker, type NodeLatencyTracker } from './node-latency-tracker';
import { createSendMessages } from './send-messages';
import type {
  CustomPartitioner,
  Message,
  ProducerBatch,
  ProducerHooks,
  ProducerRecord,
  RecordMetadata,
  TopicMessages,
} from './types';

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
   * Default 5. Pass `0` to send immediately (one Produce per `send()`).
   * @see https://kafka.apache.org/43/configuration/producer-configs/#linger.ms
   */
  lingerMs?: number;
  /**
   * Soft cap on buffered record bytes before a Produce is sent (with lingerMs).
   * Ignored when lingerMs is 0. Default 16384; pass `0` to not batch by size.
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
  /**
   * Cap on the uncompressed bytes of one Produce request. A single record over the cap rejects
   * immediately at `send`/`sendBatch` call time with `KafkaMessageTooLargeError`, before it ever
   * occupies a linger slot. Records accumulating in the linger buffer are flushed before a new
   * call would push the pending total past the cap, so no single flush groups more than
   * `maxRequestSize` bytes together. Default 1_048_576 (1 MiB), matching Java's default.
   * @see https://kafka.apache.org/43/configuration/producer-configs/#max.request.size
   */
  maxRequestSize?: number;
  /** Ordered async `onSend`/`onAck` hooks. See {@link ProducerHooks}. */
  hooks?: ProducerHooks;
  metrics?: MetricsRecorder | null;
}

export interface MessageProducer {
  send: (record: ProducerRecord & { signal?: AbortSignal }) => Promise<RecordMetadata[]>;
  sendBatch: (batch: ProducerBatch & { signal?: AbortSignal }) => Promise<RecordMetadata[]>;
  /** Send any linger-buffered records immediately. No-op when the buffer is empty. */
  flush: () => Promise<void>;
}

const DEFAULT_ACKS = -1;
const DEFAULT_TIMEOUT = 30_000;
const DEFAULT_LINGER_MS = 5;
const DEFAULT_BATCH_SIZE = 16_384;
const DEFAULT_DELIVERY_TIMEOUT_MS = 120_000;
/** Java `max.request.size` default. @see https://kafka.apache.org/43/configuration/producer-configs/#max.request.size */
const DEFAULT_MAX_REQUEST_SIZE = 1_048_576;

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
  batchSize = DEFAULT_BATCH_SIZE,
  bufferMemory,
  deliveryTimeoutMs = DEFAULT_DELIVERY_TIMEOUT_MS,
  maxRequestSize = DEFAULT_MAX_REQUEST_SIZE,
  hooks,
  metrics,
}: MessageProducerOptions): MessageProducer {
  const sendMessages = createSendMessages({
    logger,
    cluster,
    retrier,
    partitioner,
    eosManager,
    nodeLatencyTracker,
    metrics,
  });
  const pending: PendingSend[] = [];
  let lingerTimer: ReturnType<typeof setTimeout> | null = null;
  let flushInProgress: Promise<void> | null = null;
  let bufferedBytes = 0;
  // Bytes of the entries currently sitting in `pending`, i.e. what the *next* flush would group
  // into one Produce request. Distinct from `bufferedBytes`, which also counts bytes still
  // in-flight from a previous flush that hasn't released its bufferMemory reservation yet.
  let pendingBytes = 0;
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
  ): number {
    if (topicMessages.some(({ topic }) => !topic)) {
      throw new KafkaNonRetriableError('Invalid topic');
    }

    if (idempotent && acks !== -1) {
      throw new KafkaNonRetriableError(
        `Not requiring ack for all messages invalidates the idempotent producer's EoS guarantees`,
      );
    }

    let totalBytes = 0;
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

      // Single record over the cap fails fast here, at call time - it never occupies a linger
      // slot or reaches `dispatch`/the network layer.
      for (const message of messages) {
        const size = messageBytes(message);
        if (size > maxRequestSize) {
          throw new KafkaMessageTooLargeError({ size, maxRequestSize, topic });
        }
        totalBytes += size;
      }
    }

    // The whole call's records, even if individually under the cap, still have to fit in one
    // Produce request. This client doesn't split a single send/sendBatch call's records across
    // multiple requests, so - same as an over-bufferMemory call below - it rejects rather than
    // silently forwarding a request the broker would answer with MESSAGE_TOO_LARGE.
    if (totalBytes > maxRequestSize) {
      throw new KafkaMessageTooLargeError({ size: totalBytes, maxRequestSize });
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

    return totalBytes;
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
    if (lingerMs <= 0) return false;
    if (batchSize > 0 && bufferedBytes >= batchSize) return true;
    // Flush as soon as the linger buffer alone holds a full request's worth of bytes, rather
    // than letting it grow well past maxRequestSize while waiting out the rest of lingerMs.
    return pendingBytes >= maxRequestSize;
  }

  /**
   * Packs `entries` into groups whose bytes stay within `maxRequestSize`, in order. Every entry
   * fits alone (a call whose own total exceeds the cap is rejected up front in `validateBatch`),
   * so this only has to cut a *combined* linger batch back down - "send what fits, keep the rest
   * for the next request" - never split a single entry.
   */
  function chunkPendingEntries(entries: readonly PendingSend[]): PendingSend[][] {
    const chunks: PendingSend[][] = [];
    let current: PendingSend[] = [];
    let currentBytes = 0;

    for (const entry of entries) {
      if (current.length > 0 && currentBytes + entry.bytes > maxRequestSize) {
        chunks.push(current);
        current = [];
        currentBytes = 0;
      }
      current.push(entry);
      currentBytes += entry.bytes;
    }

    if (current.length > 0) chunks.push(current);
    return chunks;
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
    pendingBytes = 0;
    if (entries.length === 0) return;
    try {
      // Sequential, not Promise.all: chunks can share a topic-partition (two send() calls routed
      // to the same partition), and sending them concurrently would race their sequence-number
      // assignment for idempotent/transactional producers and could reorder delivery generally.
      for (const chunk of chunkPendingEntries(entries)) {
        await sendGrouped(chunk);
      }
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
    bytes: number,
  ): Promise<RecordMetadata[]> {
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
    pendingBytes += bytes;

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

    const totalBytes = validateBatch(topicMessages, resolvedAcks, resolvedCompression);
    const mergedTopicMessages = mergeCallTopicMessages(topicMessages);

    // `runHooks` is async, so awaiting it always costs a microtask tick even when there is
    // nothing to run. Guard on hooks being configured so a hookless producer keeps the exact
    // same synchronous-until-dispatch/enqueue timing as before this feature existed.
    const hookEvent = {
      topicMessages: mergedTopicMessages,
      acks: resolvedAcks,
      timeout: resolvedTimeout,
      compression: resolvedCompression,
    };
    if (hooks?.onSend?.length) {
      await runHooks(hooks.onSend, hookEvent, 'onSend', logger);
    }

    const produce =
      lingerMs <= 0
        ? dispatch(mergedTopicMessages, resolvedAcks, resolvedTimeout, resolvedCompression, resolvedCompressionLevel)
        : enqueue(
            mergedTopicMessages,
            resolvedAcks,
            resolvedTimeout,
            resolvedCompression,
            resolvedCompressionLevel,
            totalBytes,
          );

    const settled = rejectOnAbort(rejectOnDeliveryTimeout(produce, deliveryTimeoutMs), signal);
    if (!hooks?.onAck?.length) return settled;

    try {
      const metadata = await settled;
      await runHooks(hooks.onAck, { ...hookEvent, metadata }, 'onAck', logger);
      return metadata;
    } catch (error) {
      await runHooks(hooks.onAck, { ...hookEvent, error }, 'onAck', logger);
      throw error;
    }
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
