import type { Cluster } from '../cluster/index.js';
import { KafkaJSError, KafkaJSNonRetriableError } from '../errors.js';
import type { Logger } from '../loggers/index.js';
import { CONNECTION_STATUS, type ConnectionStatus } from '../network/connection-status.js';
import type { Retrier } from '../retry/index.js';
import type { EosManager } from './eos-manager/index.js';
import { createSendMessages } from './send-messages.js';
import type { CustomPartitioner, Message, ProducerBatch, ProducerRecord, RecordMetadata, TopicMessages } from './types.js';

export interface MessageProducerOptions {
  logger: Logger;
  cluster: Cluster;
  partitioner: ReturnType<CustomPartitioner>;
  eosManager: EosManager;
  idempotent: boolean;
  retrier: Retrier;
  getConnectionStatus: () => ConnectionStatus;
}

export interface MessageProducer {
  send: (record: ProducerRecord & { signal?: AbortSignal }) => Promise<RecordMetadata[]>;
  sendBatch: (batch: ProducerBatch & { signal?: AbortSignal }) => Promise<RecordMetadata[]>;
}

const DEFAULT_ACKS = -1;
const DEFAULT_TIMEOUT = 30_000;

/**
 * Races `promise` against `signal` firing. Doesn't cancel the underlying send (the retrier has no
 * abort hook of its own), but the caller sees a rejection as soon as they abort instead of waiting
 * out the full retry budget - the same "abort just rejects the promise" contract most Node APIs
 * that accept a bare `AbortSignal` (rather than doing real cancellation) settle for.
 */
function abortError(signal: AbortSignal): Error {
  return signal.reason instanceof Error ? signal.reason : new Error('Aborted', { cause: signal.reason as unknown });
}

function rejectOnAbort<T>(promise: Promise<T>, signal: AbortSignal | undefined): Promise<T> {
  if (!signal) return promise;
  if (signal.aborted) return Promise.reject(abortError(signal));

  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => reject(abortError(signal));
    signal.addEventListener('abort', onAbort, { once: true });
    promise.then(resolve, reject).finally(() => signal.removeEventListener('abort', onAbort));
  });
}

export function createMessageProducer({
  logger,
  cluster,
  partitioner,
  eosManager,
  idempotent,
  retrier,
  getConnectionStatus,
}: MessageProducerOptions): MessageProducer {
  const sendMessages = createSendMessages({ logger, cluster, retrier, partitioner, eosManager });

  function validateConnectionStatus(): void {
    const connectionStatus = getConnectionStatus();

    if (connectionStatus === CONNECTION_STATUS.DISCONNECTING) {
      throw new KafkaJSNonRetriableError(`The producer is disconnecting; therefore, it can't safely accept messages anymore`);
    }

    if (connectionStatus === CONNECTION_STATUS.DISCONNECTED) {
      throw new KafkaJSError('The producer is disconnected');
    }
  }

  async function sendBatch({
    acks = DEFAULT_ACKS,
    timeout = DEFAULT_TIMEOUT,
    compression,
    topicMessages = [],
    signal,
  }: ProducerBatch & { signal?: AbortSignal }): Promise<RecordMetadata[]> {
    if (topicMessages.some(({ topic }) => !topic)) {
      throw new KafkaJSNonRetriableError('Invalid topic');
    }

    if (idempotent && acks !== -1) {
      throw new KafkaJSNonRetriableError(`Not requiring ack for all messages invalidates the idempotent producer's EoS guarantees`);
    }

    for (const { topic, messages } of topicMessages) {
      if (!messages) {
        throw new KafkaJSNonRetriableError(`Invalid messages array [${String(messages)}] for topic "${topic}"`);
      }

      const messageWithoutValue = messages.find((message) => message.value === undefined);
      if (messageWithoutValue) {
        throw new KafkaJSNonRetriableError(`Invalid message without value for topic "${topic}": ${JSON.stringify(messageWithoutValue)}`);
      }
    }

    validateConnectionStatus();

    const mergedByTopic = new Map<string, Message[]>();
    for (const { topic, messages } of topicMessages) {
      const current = mergedByTopic.get(topic);
      if (current) {
        current.push(...messages);
      } else {
        mergedByTopic.set(topic, [...messages]);
      }
    }

    const mergedTopicMessages: TopicMessages[] = [...mergedByTopic.entries()].map(([topic, messages]) => ({ topic, messages }));

    return rejectOnAbort(sendMessages({ acks, timeout, compression, topicMessages: mergedTopicMessages }), signal);
  }

  async function send({ acks, timeout, compression, topic, messages, signal }: ProducerRecord & { signal?: AbortSignal }): Promise<RecordMetadata[]> {
    return sendBatch({ acks, timeout, compression, topicMessages: [{ topic, messages }], signal });
  }

  return { send, sendBatch };
}
