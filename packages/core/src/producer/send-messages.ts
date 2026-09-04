import type { Broker } from '../broker/index';
import type { Cluster } from '../cluster/index';
import { staleMetadata } from '../protocol/error-codes';
import type { CompressionType } from '../protocol/compression/index';
import { KafkaMetadataNotLoaded, KafkaProtocolError } from '../errors';
import type { Logger } from '../loggers/index';
import type { Retrier } from '../retry/index';
import type { MetricsRecorder } from '../instrumentation/metrics';
import { createTopicData } from './create-topic-data';
import type { EosManager, EosManagerPartition } from './eos-manager/index';
import { groupMessagesPerPartition } from './group-messages-per-partition';
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
}

function partitionKey(topic: string, partition: number): string {
  return `${topic}\0${partition}`;
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

function topicPartitionsFromTopicData(topicData: ReturnType<typeof createTopicData>): EosManagerPartition[] {
  const partitions: EosManagerPartition[] = [];
  for (const { topic, partitions: entries } of topicData) {
    for (const entry of entries) {
      partitions.push({ topic, partition: entry.partition });
    }
  }
  return partitions;
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
  return ({
    acks,
    timeout,
    compression,
    compressionLevel,
    topicMessages,
  }: SendMessagesRequest): Promise<RecordMetadata[]> => {
    const assignment = new Map<string, Map<number, Message[]>>();
    const ackedPartitions = new Set<string>();
    const metadataByPartition = new Map<string, RecordMetadata>();
    const produceSize = topicMessagesSize(topicMessages);

    function collectResponse(): RecordMetadata[] {
      if (acks === 0) return [];

      const result: RecordMetadata[] = [];
      for (const { topic } of topicMessages) {
        const messagesPerPartition = assignment.get(topic);
        if (!messagesPerPartition) continue;
        const partitionIds = [...messagesPerPartition.keys()].sort((a, b) => a - b);
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
      await cluster.refreshMetadataIfNecessary();
      assignMessages();

      const topicDataByBroker = new Map<Broker, ReturnType<typeof createTopicData>>();

      for (const { topic } of topicMessages) {
        const messagesPerPartition = assignment.get(topic);
        if (!messagesPerPartition) continue;

        const pendingPartitions = new Map<number, Message[]>();
        for (const [partition, messages] of messagesPerPartition) {
          if (!ackedPartitions.has(partitionKey(topic, partition))) {
            pendingPartitions.set(partition, messages);
          }
        }

        if (pendingPartitions.size === 0) continue;

        const partitionsPerLeader = cluster.findLeaderForPartitions(topic, [...pendingPartitions.keys()]);
        const topicId = cluster.findTopicId(topic);

        for (const nodeId of Object.keys(partitionsPerLeader)) {
          const broker = await cluster.findBroker({ nodeId });
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
        const topicPartitions = topicPartitionsFromTopicData(topicData);
        await eosManager.acquirePartitionGates(topicPartitions);
        try {
          if (eosManager.isTransactional()) {
            await eosManager.addPartitionsToTransaction(topicData);
          }

          for (const { topic, partitions } of topicData) {
            for (const entry of partitions) {
              entry.firstSequence = eosManager.getSequence(topic, entry.partition);
              eosManager.updateSequence(topic, entry.partition, entry.messages.length);
            }
          }

          let response;
          const producedAt = Date.now();
          const producerEpoch = eosManager.getProducerEpoch();
          try {
            response = await broker.produce({
              transactionalId: eosManager.isTransactional() ? eosManager.getTransactionalId() : undefined,
              producerId: eosManager.getProducerId(),
              producerEpoch,
              acks,
              timeout,
              compression,
              compressionLevel,
              topicData,
            });
          } catch (e) {
            // A concurrent InitProducerId (e.g. from another partition's UNKNOWN_PRODUCER_ID
            // recovery) may already have bumped the epoch and reset sequence tracking to zero
            // while this request was in flight. Rolling back against that fresh state would
            // drive it negative instead of leaving it alone, since the increment being undone
            // belongs to a generation that no longer exists.
            if (eosManager.getProducerEpoch() === producerEpoch) {
              for (const { topic, partitions } of topicData) {
                for (const entry of partitions) {
                  eosManager.updateSequence(topic, entry.partition, -entry.messages.length);
                }
              }
            }
            throw e;
          }

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
            for (const { topic, partition } of topicPartitions) {
              ackedPartitions.add(partitionKey(topic, partition));
            }
          }
        } finally {
          await eosManager.releasePartitionGates(topicPartitions);
        }
      });
    }

    return retrier(async (bail, retryCount, retryTime) => {
      const topics = topicMessages.map(({ topic }) => topic);
      await cluster.addMultipleTargetTopics(topics);

      try {
        const requests = await createProducerRequests();
        const results = await Promise.allSettled(requests);
        const rejection = results.find((result): result is PromiseRejectedResult => result.status === 'rejected');
        if (rejection) throw rejection.reason;
        const collected = collectResponse();
        metrics?.recordProduce({ ...produceSize, retries: retryCount });
        cluster.recordProduceMetrics?.({ ...produceSize, retries: retryCount });
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

        // UNKNOWN_PRODUCER_ID is not marked retriable: the broker dropped this PID after
        // retention. KIP-360 lets a v3+ InitProducerId bump the epoch so produce can continue.
        if (error.type === 'UNKNOWN_PRODUCER_ID' && eosManager.isInitialized()) {
          logger.warn(`Producer id was fenced or expired; reallocating: ${error.message}`, {
            retryCount,
            retryTime,
          });
          await eosManager.initProducerId();
          throw new KafkaProtocolError(error, { retriable: true });
        }

        // KIP-890: the broker rejected this produce and the transaction can no longer commit,
        // only abort. Mark it so a later `commit()` fails clearly instead of racing the broker.
        if (error.type === 'TRANSACTION_ABORTABLE') {
          eosManager.markTransactionAbortable();
        }

        logger.error(`${error.message}`, { retryCount, retryTime });
        if (error.retriable) throw error;
        bail(error);
        return collectResponse();
      }
    });
  };
}
