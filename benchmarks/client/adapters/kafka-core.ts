import { CompressionTypes, Kafka, logLevel } from '@cookiemonsterdev/kafka-core';
import type {
  Consumer,
  ConsumerConfig,
  EachBatchPayload,
  EachMessagePayload,
  KafkaMessage,
  ProducerConfig,
} from '@cookiemonsterdev/kafka-core';
import { clientPackageNames, type WireSettings } from '../spec.ts';
import { createOffsetCommitter, nextOffsetAfter, readInstalledVersion, sumValueBytes } from './shared.ts';
import type { AdapterOptions, BenchConsumer, BenchProducer, ClientAdapter, ConsumeOptions } from './types.ts';

const packageName = clientPackageNames['kafka-core'];

function createKafka(options: AdapterOptions): Kafka {
  return new Kafka({ brokers: options.brokers, clientId: options.clientId, logLevel: logLevel.NOTHING });
}

function producerConfig(wire: WireSettings): ProducerConfig {
  return {
    acks: wire.acks,
    lingerMs: wire.lingerMs,
    batchSize: wire.batchBytes,
    compression: wire.compression === 'gzip' ? CompressionTypes.GZIP : CompressionTypes.None,
    maxInFlightRequests: wire.maxInFlight,
    idempotent: wire.idempotent,
  };
}

function consumerConfig(groupId: string, wire: WireSettings): ConsumerConfig {
  return {
    groupId,
    minBytes: wire.fetchMinBytes,
    maxBytes: wire.fetchMaxBytes,
    maxWaitTimeInMs: wire.fetchMaxWaitMs,
    maxBytesPerPartition: wire.partitionFetchMaxBytes,
  };
}

function createProducer(options: AdapterOptions): BenchProducer {
  const kafka = createKafka(options);
  const producer = kafka.producer(options.profile === 'normalized' ? producerConfig(options.wire) : {});
  return {
    connect: () => producer.connect(),
    async send(topic, records) {
      await producer.send({ topic, messages: records });
    },
    disconnect: () => producer.disconnect(),
  };
}

function createConsumer(options: AdapterOptions): BenchConsumer {
  const kafka = createKafka(options);
  let consumer: Consumer | null = null;

  return {
    async connect() {
      // kafka-core needs the groupId to build a consumer, and it only arrives with start().
    },
    async start(consume: ConsumeOptions) {
      const isNormalized = options.profile === 'normalized';
      consumer = kafka.consumer(
        isNormalized ? consumerConfig(consume.groupId, options.wire) : { groupId: consume.groupId },
      );
      await consumer.connect();
      await consumer.subscribe({ topics: [consume.topic], fromBeginning: true });

      const activeConsumer = consumer;
      const committer =
        consume.commitEvery > 0
          ? createOffsetCommitter<KafkaMessage>(consume.commitEvery, (lastRecords) =>
              activeConsumer.commitOffsets(
                [...lastRecords].map(([partition, message]) => ({
                  topic: consume.topic,
                  partition,
                  offset: nextOffsetAfter(message.offset),
                })),
              ),
            )
          : null;
      // Explicit commits only, unless the defaults profile leaves the library's auto-commit on.
      const autoCommit = committer === null && !isNormalized;

      if (consume.mode === 'each-batch') {
        await consumer.run({
          autoCommit,
          eachBatch: async ({ batch }: EachBatchPayload) => {
            consume.onBatch(batch.messages.length, sumValueBytes(batch.messages));
            const lastMessage = batch.messages[batch.messages.length - 1];
            if (committer === null || lastMessage === undefined) {
              return;
            }
            committer.seen(batch.partition, lastMessage);
            const commit = committer.delivered(batch.messages.length);
            if (commit !== null) {
              await commit;
            }
          },
        });
        return;
      }

      await consumer.run({
        autoCommit,
        eachMessage: async ({ partition, message }: EachMessagePayload) => {
          consume.onMessage(message.value);
          if (committer === null) {
            return;
          }
          committer.seen(partition, message);
          const commit = committer.delivered(1);
          if (commit !== null) {
            await commit;
          }
        },
      });
    },
    async disconnect() {
      await consumer?.disconnect();
    },
  };
}

export const adapter: ClientAdapter = {
  id: 'kafka-core',
  packageName,
  version: () => readInstalledVersion(packageName),
  // Every WireSettings field maps to a kafka-core option, so nothing is missing under either profile.
  capabilityGaps: () => [],
  createProducer,
  createConsumer,
};

export default adapter;
