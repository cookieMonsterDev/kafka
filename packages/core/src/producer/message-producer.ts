import { supportsHeaders, supportsZstd } from '../broker/capabilities';
import type { Cluster } from '../cluster/index';
import { KafkaError, KafkaMessageTooLargeError, KafkaNonRetriableError } from '../errors';
import type { MetricsRecorder } from '../instrumentation/metrics';
import type { Logger } from '../loggers/index';
import { CONNECTION_STATUS, type ConnectionStatus } from '../network/connection-status';
import { COMPRESSION_TYPES, type CompressionType } from '../protocol/compression/index';
import type { Retrier } from '../retry/index';
import { rejectOnAbort } from '../utils/abort';
import { runHooks } from '../utils/run-hooks';
import type { EosManager } from './eos-manager/index';
import { mergeTopicMessages } from './merge-topic-messages';
import { createNodeLatencyTracker, type NodeLatencyTracker } from './node-latency-tracker';
import { createRecordAccumulator } from './record-accumulator';
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
   * Delay in ms to wait for more records before sending a Produce request. Default 5. `0` adds no
   * delay: calls made in the same event-loop turn still leave together, as one Produce per broker.
   * @see https://kafka.apache.org/43/configuration/producer-configs/#linger.ms
   */
  lingerMs?: number;
  /**
   * Flush threshold, in record key and value bytes, for the linger buffer. Unlike Java's
   * per-partition `batch.size`, it counts the producer's total across all topics and partitions,
   * including records from an earlier flush still awaiting their response. The buffer flushes as
   * soon as that total reaches `batchSize`, without waiting out `lingerMs`. Ignored when lingerMs
   * is 0. Default 16384; pass `0` to not flush by size.
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
   * No retry starts after the deadline; an attempt already in flight is left to finish.
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

function fieldBytes(value: Buffer | string | null | undefined): number {
  if (value == null) return 0;
  return Buffer.byteLength(value);
}

function messageBytes(message: Message): number {
  return fieldBytes(message.key) + fieldBytes(message.value);
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
  const accumulator = createRecordAccumulator({
    dispatch: sendMessages,
    lingerMs,
    batchSize,
    bufferMemory,
    deliveryTimeoutMs,
    maxRequestSize,
  });

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
      // slot or reaches the network layer.
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
    const mergedTopicMessages = mergeTopicMessages(topicMessages);

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

    const produce = accumulator.append({
      topicMessages: mergedTopicMessages,
      acks: resolvedAcks,
      timeout: resolvedTimeout,
      compression: resolvedCompression,
      compressionLevel: resolvedCompressionLevel,
      bytes: totalBytes,
    });

    const settled = rejectOnAbort(produce, signal);
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

  return { send, sendBatch, flush: accumulator.flush };
}
