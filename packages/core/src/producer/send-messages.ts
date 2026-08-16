import type { Broker } from '../broker/index.js';
import type { Cluster } from '../cluster/index.js';
import { staleMetadata } from '../protocol/error-codes.js';
import type { CompressionType } from '../protocol/compression/index.js';
import { KafkaJSMetadataNotLoaded } from '../errors.js';
import type { Logger } from '../loggers/index.js';
import type { Retrier } from '../retry/index.js';
import { createTopicData } from './create-topic-data.js';
import type { EosManager } from './eos-manager/index.js';
import { groupMessagesPerPartition } from './group-messages-per-partition.js';
import { responseSerializer } from './response-serializer.js';
import type { CustomPartitioner, Message, RecordMetadata, TopicMessages } from './types.js';

export interface SendMessagesOptions {
  logger: Logger;
  cluster: Cluster;
  partitioner: ReturnType<CustomPartitioner>;
  eosManager: EosManager;
  retrier: Retrier;
}

export interface SendMessagesRequest {
  /** Always supplied by `message-producer.ts`'s `send`/`sendBatch`, which apply the public default of `-1`. */
  acks: number;
  timeout: number;
  compression?: CompressionType;
  topicMessages: readonly TopicMessages[];
}

interface TopicRequestMetadata {
  partitionsPerLeader: Record<number, number[]>;
  messagesPerPartition: Map<number, Message[]>;
}

export function createSendMessages({ logger, cluster, partitioner, eosManager, retrier }: SendMessagesOptions) {
  return ({ acks, timeout, compression, topicMessages }: SendMessagesRequest): Promise<RecordMetadata[]> => {
    const responsePerBroker = new Map<Broker, RecordMetadata[] | null>();

    async function createProducerRequests(): Promise<Promise<void>[]> {
      const topicMetadata = new Map<string, TopicRequestMetadata>();

      await cluster.refreshMetadataIfNecessary();

      for (const { topic, messages } of topicMessages) {
        const partitionMetadata = cluster.findTopicPartitionMetadata(topic);

        if (partitionMetadata.length === 0) {
          logger.debug('Producing to topic without metadata', { topic, targetTopics: [...cluster.targetTopics] });
          throw new KafkaJSMetadataNotLoaded('Producing to topic without metadata');
        }

        const messagesPerPartition = groupMessagesPerPartition({ topic, partitionMetadata, messages, partitioner });
        const partitionsPerLeader = cluster.findLeaderForPartitions(topic, [...messagesPerPartition.keys()]);

        topicMetadata.set(topic, { partitionsPerLeader, messagesPerPartition });

        for (const nodeId of Object.keys(partitionsPerLeader)) {
          const broker = await cluster.findBroker({ nodeId });
          if (!responsePerBroker.has(broker)) {
            responsePerBroker.set(broker, null);
          }
        }
      }

      const brokersWithoutResponse = [...responsePerBroker.keys()].filter((broker) => !responsePerBroker.get(broker));

      return brokersWithoutResponse.map(async (broker) => {
        const topicDataForBroker = [...topicMetadata.entries()]
          .filter(([, { partitionsPerLeader }]) => broker.nodeId != null && partitionsPerLeader[broker.nodeId])
          .map(([topic, { partitionsPerLeader, messagesPerPartition }]) => ({
            topic,
            partitions: (broker.nodeId != null ? partitionsPerLeader[broker.nodeId] : undefined) ?? [],
            messagesPerPartition,
          }));

        const topicData = createTopicData(topicDataForBroker);

        await eosManager.acquireBrokerLock(broker);
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
          try {
            response = await broker.produce({
              transactionalId: eosManager.isTransactional() ? eosManager.getTransactionalId() : undefined,
              producerId: eosManager.getProducerId(),
              producerEpoch: eosManager.getProducerEpoch(),
              acks,
              timeout,
              compression,
              topicData,
            });
          } catch (e) {
            for (const { topic, partitions } of topicData) {
              for (const entry of partitions) {
                eosManager.updateSequence(topic, entry.partition, -entry.messages.length);
              }
            }
            throw e;
          }

          const expectsResponse = acks !== 0;
          const formattedResponse = expectsResponse && response ? responseSerializer(response) : [];

          responsePerBroker.set(broker, formattedResponse);
        } catch (e) {
          responsePerBroker.delete(broker);
          throw e;
        } finally {
          await eosManager.releaseBrokerLock(broker);
        }
      });
    }

    return retrier(async (bail, retryCount, retryTime) => {
      const topics = topicMessages.map(({ topic }) => topic);
      await cluster.addMultipleTargetTopics(topics);

      try {
        const requests = await createProducerRequests();
        await Promise.all(requests);
        return [...responsePerBroker.values()].flatMap((response) => response ?? []);
      } catch (e) {
        const error = e as Error & { name: string; host?: string; port?: number; retriable?: boolean; type?: string };

        if (error.name === 'KafkaJSConnectionClosedError' && error.host != null && error.port != null) {
          cluster.removeBroker({ host: error.host, port: error.port });
        }

        if (!cluster.isConnected()) {
          logger.debug(`Cluster has disconnected, reconnecting: ${error.message}`, { retryCount, retryTime });
          await cluster.connect();
          await cluster.refreshMetadata();
          throw error;
        }

        // This is necessary in case the metadata is stale and the number of partitions for this
        // topic has increased in the meantime.
        if (
          staleMetadata(error) ||
          error.name === 'KafkaJSMetadataNotLoaded' ||
          error.name === 'KafkaJSConnectionError' ||
          error.name === 'KafkaJSConnectionClosedError' ||
          (error.name === 'KafkaJSProtocolError' && error.retriable)
        ) {
          logger.error(`Failed to send messages: ${error.message}`, { retryCount, retryTime });
          await cluster.refreshMetadata();
          throw error;
        }

        logger.error(`${error.message}`, { retryCount, retryTime });
        if (error.retriable) throw error;
        bail(error);
        return [];
      }
    });
  };
}
