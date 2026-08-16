import { supportsHeaders, supportsZstd } from '../broker/capabilities';
import type { Cluster } from '../cluster/index';
import { KafkaError, KafkaNonRetriableError } from '../errors';
import type { Logger } from '../loggers/index';
import { CONNECTION_STATUS, type ConnectionStatus } from '../network/connection-status';
import { COMPRESSION_TYPES, type CompressionType } from '../protocol/compression/index';
import type { Retrier } from '../retry/index';
import { rejectOnAbort } from '../utils/abort';
import type { EosManager } from './eos-manager/index';
import { createSendMessages } from './send-messages';
import type { CustomPartitioner, Message, ProducerBatch, ProducerRecord, RecordMetadata, TopicMessages } from './types';

export interface MessageProducerOptions {
  logger: Logger;
  cluster: Cluster;
  partitioner: ReturnType<CustomPartitioner>;
  eosManager: EosManager;
  idempotent: boolean;
  retrier: Retrier;
  getConnectionStatus: () => ConnectionStatus;
  /** Used when send/sendBatch omit acks. Falls back to -1 (all ISR). */
  defaultAcks?: number;
  /** Used when send/sendBatch omit compression. Omit for none. */
  defaultCompression?: CompressionType;
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

interface PendingSend {
  topicMessages: TopicMessages[];
  topics: Set<string>;
  acks: number;
  compression: CompressionType | undefined;
  timeout: number;
  resolve: (metadata: RecordMetadata[]) => void;
  reject: (error: unknown) => void;
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
  return `${entry.acks}\0${compression}\0${entry.timeout}`;
}

export function createMessageProducer({
  logger,
  cluster,
  partitioner,
  eosManager,
  idempotent,
  retrier,
  getConnectionStatus,
  defaultAcks,
  defaultCompression,
  lingerMs = DEFAULT_LINGER_MS,
  batchSize = 0,
}: MessageProducerOptions): MessageProducer {
  const sendMessages = createSendMessages({ logger, cluster, retrier, partitioner, eosManager });
  const pending: PendingSend[] = [];
  let lingerTimer: ReturnType<typeof setTimeout> | null = null;
  let flushInProgress: Promise<void> | null = null;

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
  ): Promise<RecordMetadata[]> {
    return sendMessages({ acks, timeout, compression, topicMessages });
  }

  function estimatedPendingBytes(): number {
    let total = 0;
    for (const entry of pending) {
      total += topicMessagesBytes(entry.topicMessages);
    }
    return total;
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
    return lingerMs > 0 && batchSize > 0 && estimatedPendingBytes() >= batchSize;
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

    for (const group of groups.values()) {
      const first = group[0];
      if (!first) continue;

      try {
        const metadata = await dispatch(mergeTopicMessages(group), first.acks, first.timeout, first.compression);
        for (const entry of group) {
          entry.resolve(metadata.filter((item) => entry.topics.has(item.topicName)));
        }
      } catch (error) {
        for (const entry of group) {
          entry.reject(error);
        }
      }
    }
  }

  async function doFlush(): Promise<void> {
    clearLingerTimer();
    const entries = pending.splice(0);
    if (entries.length === 0) return;
    await sendGrouped(entries);
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

  function enqueue(
    topicMessages: TopicMessages[],
    acks: number,
    timeout: number,
    compression: CompressionType | undefined,
  ): Promise<RecordMetadata[]> {
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
      timeout,
      resolve,
      reject,
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
    topicMessages = [],
    signal,
  }: ProducerBatch & { signal?: AbortSignal }): Promise<RecordMetadata[]> {
    const resolvedAcks = acks ?? defaultAcks ?? DEFAULT_ACKS;
    const resolvedTimeout = timeout ?? DEFAULT_TIMEOUT;
    const resolvedCompression = compression ?? defaultCompression;

    validateBatch(topicMessages, resolvedAcks, resolvedCompression);
    const mergedTopicMessages = mergeCallTopicMessages(topicMessages);

    const produce =
      lingerMs <= 0
        ? dispatch(mergedTopicMessages, resolvedAcks, resolvedTimeout, resolvedCompression)
        : enqueue(mergedTopicMessages, resolvedAcks, resolvedTimeout, resolvedCompression);

    return rejectOnAbort(produce, signal);
  }

  async function send({
    acks,
    timeout,
    compression,
    topic,
    messages,
    signal,
  }: ProducerRecord & { signal?: AbortSignal }): Promise<RecordMetadata[]> {
    return sendBatch({ acks, timeout, compression, topicMessages: [{ topic, messages }], signal });
  }

  return { send, sendBatch, flush };
}
