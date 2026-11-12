import kafkajs from 'kafkajs';
import type {
  Consumer,
  ConsumerConfig,
  EachBatchPayload,
  EachMessagePayload,
  Kafka,
  KafkaMessage,
  ProducerConfig,
  ProducerRecord,
} from 'kafkajs';
import { clientPackageNames, type ScenarioDefinition, type WireSettings } from '../spec.ts';
import type {
  AdapterOptions,
  BenchConsumer,
  BenchProducer,
  BenchRecord,
  ClientAdapter,
  ConsumeOptions,
} from './types.ts';
import {
  createOffsetCommitter,
  nextOffsetAfter,
  perSendBatchingGaps,
  readInstalledVersion,
  sumValueBytes,
} from './shared.ts';

// kafkajs is CommonJS; its default import is `module.exports` under Node and Bun alike.
const { CompressionTypes, logLevel } = kafkajs;

const packageName = clientPackageNames.kafkajs;

function createKafka(options: AdapterOptions): Kafka {
  return new kafkajs.Kafka({ brokers: options.brokers, clientId: options.clientId, logLevel: logLevel.NOTHING });
}

function producerConfig(wire: WireSettings): ProducerConfig {
  return { idempotent: wire.idempotent, maxInFlightRequests: wire.maxInFlight };
}

// kafkajs takes acks and compression per send() call, not per producer.
function sendSettings(wire: WireSettings): Pick<ProducerRecord, 'acks' | 'compression'> {
  return {
    acks: wire.acks,
    compression: wire.compression === 'gzip' ? CompressionTypes.GZIP : CompressionTypes.None,
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
  const isNormalized = options.profile === 'normalized';
  const producer = kafka.producer(isNormalized ? producerConfig(options.wire) : {});
  const settings = isNormalized ? sendSettings(options.wire) : {};
  return {
    connect: () => producer.connect(),
    async send(topic, records) {
      // kafkajs 2.2.4 only reads the messages and array it is given (src/producer), so the pooled records go in
      // as they are; the cast only drops `readonly`.
      await producer.send({ topic, messages: records as BenchRecord[], ...settings });
    },
    disconnect: () => producer.disconnect(),
  };
}

function createConsumer(options: AdapterOptions): BenchConsumer {
  const kafka = createKafka(options);
  let consumer: Consumer | null = null;
  let crashError: Error | null = null;

  return {
    async connect() {
      // kafkajs needs the groupId to build a consumer, and that only arrives with start().
    },
    async start(consume: ConsumeOptions) {
      const isNormalized = options.profile === 'normalized';
      const activeConsumer = kafka.consumer(
        isNormalized ? consumerConfig(consume.groupId, options.wire) : { groupId: consume.groupId },
      );
      consumer = activeConsumer;
      activeConsumer.on(activeConsumer.events.CRASH, (event) => {
        crashError = event.payload.error;
      });
      await activeConsumer.connect();
      await activeConsumer.subscribe({ topics: [consume.topic], fromBeginning: true });

      const committer =
        consume.commitEvery > 0
          ? createOffsetCommitter<KafkaMessage>(consume.commitEvery, (lastRecords) =>
              activeConsumer.commitOffsets(
                [...lastRecords].map(([partition, message]) => ({
                  topic: consume.topic,
                  partition,
                  offset: nextOffsetAfter(message.offset).toString(),
                })),
              ),
            )
          : null;
      // Explicit commits only, unless the defaults profile leaves the library's auto-commit on.
      const autoCommit = committer === null && !isNormalized;

      if (consume.mode === 'each-batch') {
        await activeConsumer.run({
          autoCommit,
          eachBatch: async ({ batch }: EachBatchPayload) => {
            const lastMessage = batch.messages[batch.messages.length - 1];
            if (lastMessage === undefined) {
              return;
            }
            consume.onBatch(batch.messages.length, sumValueBytes(batch.messages));
            if (committer === null) {
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

      await activeConsumer.run({
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
      if (crashError !== null) {
        throw new Error(`kafkajs consumer crashed: ${crashError.message}`, { cause: crashError });
      }
    },
  };
}

function capabilityGaps(options: AdapterOptions, scenario: ScenarioDefinition): string[] {
  if (options.profile !== 'normalized' || scenario.kind === 'consume') {
    return [];
  }
  return perSendBatchingGaps(options.wire);
}

export const adapter: ClientAdapter = {
  id: 'kafkajs',
  packageName,
  version: () => readInstalledVersion(packageName),
  capabilityGaps,
  createProducer,
  createConsumer,
};

export default adapter;
