import type { Broker } from '../broker/index';
import type { Cluster } from '../cluster/index';
import { staleMetadata } from '../protocol/error-codes';
import type { CompressionType } from '../protocol/compression/index';
import { KafkaMetadataNotLoaded, KafkaProtocolError } from '../errors';
import type { Logger } from '../loggers/index';
import type { Retrier, Retryable } from '../retry/index';
import type { MetricsRecorder } from '../instrumentation/metrics';
import type { Deadline } from '../utils/deadline';
import { createTopicData } from './create-topic-data';
import type { EosManager } from './eos-manager/index';
import { groupMessagesPerPartition } from './group-messages-per-partition';
import { partitionKey } from './idempotence-ledger';
import { createNodeLatencyTracker, type NodeLatencyTracker } from './node-latency-tracker';
import { responseSerializer } from './response-serializer';
import type { CustomPartitioner, Message, RecordMetadata, TopicMessages } from './types';

export interface SendMessagesOptions {
  logger: Logger;
  cluster: Cluster;
  partitioner: ReturnType<CustomPartitioner>;
  eosManager: EosManager;
  retrier: Retrier;
  /** Shared across a producer's lifetime. Defaults to a fresh tracker. */
  nodeLatencyTracker?: NodeLatencyTracker;
  metrics?: MetricsRecorder | null;
}

export interface SendMessagesRequest {
  /** Always supplied by `message-producer.ts`'s `send`/`sendBatch`, which apply the public default of `-1`. */
  acks: number;
  timeout: number;
  compression?: CompressionType;
  compressionLevel?: number;
  topicMessages: readonly TopicMessages[];
  /** The delivery deadline. Once it expires, no new attempt starts and queued gate waits give up. */
  deadline?: Deadline;
}

function payloadBytes(value: Message['key'] | Message['value']): number {
  if (value == null) return 0;
  if (Buffer.isBuffer(value)) return value.byteLength;
  return Buffer.byteLength(String(value));
}

function topicMessagesSize(topicMessages: readonly TopicMessages[]): { records: number; bytes: number } {
  let records = 0;
  let bytes = 0;
  for (const { messages } of topicMessages) {
    for (const message of messages) {
      records += 1;
      bytes += payloadBytes(message.key) + payloadBytes(message.value);
    }
  }
  return { records, bytes };
}

/** Errors the broker returns although the records may already be on the leader's log. */
const WRITE_UNKNOWN_ERROR_TYPES = new Set([
  'REQUEST_TIMED_OUT',
  'NOT_ENOUGH_REPLICAS_AFTER_APPEND',
  'OUT_OF_ORDER_SEQUENCE_NUMBER',
]);

/**
 * Whether the broker definitely did not append this partition's records: only a partition-level
 * error names it. Anything else (a timeout, a dropped connection, a sibling's error failing the
 * whole response) leaves the outcome unknown.
 */
function wasRejected(error: unknown, topic: string, partition: number): boolean {
  return (
    error instanceof KafkaProtocolError &&
    error.topic === topic &&
    error.partition === partition &&
    !WRITE_UNKNOWN_ERROR_TYPES.has(error.type ?? '')
  );
}

export function createSendMessages({
  logger,
  cluster,
  partitioner,
  eosManager,
  retrier,
  nodeLatencyTracker = createNodeLatencyTracker(),
  metrics,
}: SendMessagesOptions) {
  const { ledger } = eosManager;

  return ({
    acks,
    timeout,
    compression,
    compressionLevel,
    topicMessages,
    deadline,
  }: SendMessagesRequest): Promise<RecordMetadata[]> => {
    const assignment = new Map<string, Map<number, Message[]>>();
    const ackedPartitions = new Set<string>();
    const metadataByPartition = new Map<string, RecordMetadata>();
    const produceSize = topicMessagesSize(topicMessages);
    const topicNames = topicMessages.map(({ topic }) => topic);
    /**
     * Holds this send's partition gates and sequences from its first attempt until it settles, so a
     * queued send cannot take sequences ahead of this one's retries. Reads `assignment` once filled.
     */
    const lease = ledger.lease(assignment);

    function collectResponse(): RecordMetadata[] {
      if (acks === 0) return [];

      const result: RecordMetadata[] = [];
      for (const { topic } of topicMessages) {
        const messagesPerPartition = assignment.get(topic);
        if (!messagesPerPartition) continue;
        const partitionIds =
          messagesPerPartition.size === 1
            ? messagesPerPartition.keys()
            : [...messagesPerPartition.keys()].sort((a, b) => a - b);
        for (const partition of partitionIds) {
          const metadata = metadataByPartition.get(partitionKey(topic, partition));
          if (metadata) result.push(metadata);
        }
      }
      return result;
    }

    function assignMessages(): void {
      if (assignment.size > 0) return;

      const next = new Map<string, Map<number, Message[]>>();
      for (const { topic, messages } of topicMessages) {
        const partitionMetadata = cluster.findTopicPartitionMetadata(topic);

        if (partitionMetadata.length === 0) {
          logger.debug('Producing to topic without metadata', { topic, targetTopics: [...cluster.targetTopics] });
          throw new KafkaMetadataNotLoaded('Producing to topic without metadata');
        }

        next.set(
          topic,
          groupMessagesPerPartition({
            topic,
            partitionMetadata,
            messages,
            partitioner,
            nodeLatency: nodeLatencyTracker,
          }),
        );
      }

      for (const [topic, messagesPerPartition] of next) {
        assignment.set(topic, messagesPerPartition);
      }
    }

    async function createProducerRequests(): Promise<Promise<void>[]> {
      if (cluster.needsMetadataRefresh()) await cluster.refreshMetadataIfNecessary();
      assignMessages();
      const gateWait = lease.acquire(deadline);
      if (gateWait) await gateWait;

      const topicDataByBroker = new Map<Broker, ReturnType<typeof createTopicData>>();

      for (const { topic } of topicMessages) {
        const messagesPerPartition = assignment.get(topic);
        if (!messagesPerPartition) continue;

        // Nothing is acked before the first attempt, so it sends every assigned partition as is.
        let pendingPartitions = messagesPerPartition;
        if (ackedPartitions.size > 0) {
          pendingPartitions = new Map<number, Message[]>();
          for (const [partition, messages] of messagesPerPartition) {
            if (!ackedPartitions.has(partitionKey(topic, partition))) {
              pendingPartitions.set(partition, messages);
            }
          }
        }

        if (pendingPartitions.size === 0) continue;

        const partitionsPerLeader = cluster.findLeaderForPartitions(topic, [...pendingPartitions.keys()]);
        const topicId = cluster.findTopicId(topic);

        for (const nodeId of Object.keys(partitionsPerLeader)) {
          const broker = cluster.connectedBroker(nodeId) ?? (await cluster.findBroker({ nodeId }));
          const leaderPartitions = (broker.nodeId != null ? partitionsPerLeader[broker.nodeId] : undefined) ?? [];
          const partitions = leaderPartitions.filter((partition) => pendingPartitions.has(partition));
          if (partitions.length === 0) continue;

          const current = topicDataByBroker.get(broker) ?? [];
          current.push(
            ...createTopicData([
              {
                topic,
                topicId,
                partitions,
                messagesPerPartition: pendingPartitions,
              },
            ]),
          );
          topicDataByBroker.set(broker, current);
        }
      }

      return [...topicDataByBroker.entries()].map(async ([broker, topicData]) => {
        if (eosManager.isTransactional()) {
          await eosManager.addPartitionsToTransaction(topicData);
        }

        // Without a producer id, sequences are not tracked and the encoder writes the default.
        if (ledger.isInitialized()) {
          for (const { topic, partitions } of topicData) {
            for (const entry of partitions) entry.firstSequence = lease.firstSequence(topic, entry.partition);
          }
        }

        const producedAt = Date.now();
        const response = await broker
          .produce({
            transactionalId: eosManager.isTransactional() ? eosManager.getTransactionalId() : undefined,
            producerId: ledger.getProducerId(),
            producerEpoch: ledger.getProducerEpoch(),
            acks,
            timeout,
            compression,
            compressionLevel,
            topicData,
          })
          .catch((error: unknown) => {
            const maybeWritten: string[] = [];
            for (const { topic, partitions } of topicData) {
              for (const { partition } of partitions) {
                if (!wasRejected(error, topic, partition)) maybeWritten.push(partitionKey(topic, partition));
              }
            }
            lease.markMaybeWritten(maybeWritten);
            throw error;
          });

        const expectsResponse = acks !== 0;
        // acks: 0 doesn't wait on the broker at all, so timing it would measure the socket
        // write, not the node's responsiveness - only record when a response was awaited.
        if (expectsResponse && broker.nodeId != null) {
          nodeLatencyTracker.record(broker.nodeId, Date.now() - producedAt);
        }
        const formattedResponse = expectsResponse && response ? responseSerializer(response) : [];

        if (expectsResponse) {
          for (const metadata of formattedResponse) {
            const key = partitionKey(metadata.topicName, metadata.partition);
            ackedPartitions.add(key);
            metadataByPartition.set(key, metadata);
          }
        } else {
          for (const { topic, partitions } of topicData) {
            for (const { partition } of partitions) ackedPartitions.add(partitionKey(topic, partition));
          }
        }
      });
    }

    // `hasLoadedTargetTopics`, `needsMetadataRefresh` and `connectedBroker` repeat the first check of the
    // async method each guards, so a send with fresh metadata and connected leaders skips those awaits.
    const attempt: Retryable<RecordMetadata[]> = async (bail, retryCount, retryTime) => {
      if (!cluster.hasLoadedTargetTopics(topicNames)) await cluster.addMultipleTargetTopics(topicNames);

      try {
        const requests = await createProducerRequests();
        if (requests.length === 1) {
          await requests[0];
        } else {
          const results = await Promise.allSettled(requests);
          const rejection = results.find((result): result is PromiseRejectedResult => result.status === 'rejected');
          if (rejection) throw rejection.reason;
        }
        const collected = collectResponse();
        const produceMetrics = { ...produceSize, retries: retryCount };
        metrics?.recordProduce(produceMetrics);
        cluster.recordProduceMetrics?.(produceMetrics);
        return collected;
      } catch (e) {
        const error = e as Error & {
          name: string;
          host?: string;
          port?: number;
          retriable?: boolean;
          type?: string;
          topic?: string;
          partition?: number;
          currentLeader?: { leaderId: number; leaderEpoch: number };
          nodeEndpoints?: { nodeId: number; host: string; port: number; rack: string | null }[];
        };

        if (error.name === 'KafkaConnectionClosedError' && error.host != null && error.port != null) {
          cluster.removeBroker({ host: error.host, port: error.port });
        }

        if (!cluster.isConnected()) {
          logger.debug(`Cluster has disconnected, reconnecting: ${error.message}`, { retryCount, retryTime });
          await cluster.connect();
          await cluster.refreshMetadata();
          throw error;
        }

        // KIP-951: the Produce response itself named the new leader (and its address, if the
        // client didn't already have it cached). Patch the cache locally instead of paying for a
        // full Metadata round trip before the retrier's next attempt.
        if (
          staleMetadata(error) &&
          error.topic != null &&
          error.partition != null &&
          error.currentLeader != null &&
          error.currentLeader.leaderId >= 0
        ) {
          const patched = await cluster.applyLeaderUpdate({
            topic: error.topic,
            partition: error.partition,
            currentLeader: error.currentLeader,
            nodeEndpoints: error.nodeEndpoints ?? [],
          });

          if (patched) {
            logger.debug(`Recovered leader from Produce response, skipping metadata refresh: ${error.message}`, {
              retryCount,
              retryTime,
              topic: error.topic,
              partition: error.partition,
              leaderId: error.currentLeader.leaderId,
            });
            throw error;
          }
        }

        // This is necessary in case the metadata is stale and the number of partitions for this
        // topic has increased in the meantime.
        if (
          staleMetadata(error) ||
          error.name === 'KafkaMetadataNotLoaded' ||
          error.name === 'KafkaConnectionError' ||
          error.name === 'KafkaConnectionClosedError' ||
          error.name === 'KafkaRequestTimeoutError' ||
          (error.name === 'KafkaProtocolError' && error.retriable)
        ) {
          logger.error(`Failed to send messages: ${error.message}`, { retryCount, retryTime });
          await cluster.refreshMetadata();
          throw error;
        }

        // UNKNOWN_PRODUCER_ID is not marked retriable: the broker dropped this producer id after
        // retention. KIP-360 lets a v3+ InitProducerId bump the epoch so produce can continue, once
        // every other send has drained. A transaction can only abort, which bumps it then.
        if (error.type === 'UNKNOWN_PRODUCER_ID') {
          const canRetry = lease.retryUnderNewEpoch();
          if (canRetry) {
            logger.warn(`Producer id was fenced or expired; reallocating: ${error.message}`, {
              retryCount,
              retryTime,
            });
            throw new KafkaProtocolError(error, { retriable: true });
          }
        }

        logger.error(`${error.message}`, { retryCount, retryTime });
        if (error.retriable) throw error;
        bail(error);
        return collectResponse();
      }
    };

    return retrier(attempt, { deadline }).then(
      (metadata) => {
        lease.release();
        return metadata;
      },
      (error: unknown) => {
        lease.release(ackedPartitions);
        // Some partitions may already be written inside the transaction (and KIP-890's
        // TRANSACTION_ABORTABLE says so outright), so it may only abort from here, like the Java
        // client's abortable error state; a later `commit()` must not keep a partial send.
        if (eosManager.isTransactional()) eosManager.markTransactionAbortable();
        throw error;
      },
    );
  };
}
