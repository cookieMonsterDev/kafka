import confluent from '@confluentinc/kafka-javascript';
import type { KafkaJS } from '@confluentinc/kafka-javascript';
import { clientPackageNames, type WireSettings } from '../spec.ts';
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
  readInstalledVersion,
  sumValueBytes,
  unverifiedCrcGaps,
} from './shared.ts';

// CommonJS package with a native addon: the default import is `module.exports`, and importing it fails when
// the addon is missing for this runtime, which the harness records as unsupported.
const { Kafka, logLevel } = confluent.KafkaJS;

const packageName = clientPackageNames.confluent;

// Identity and logging go in the `kafkaJS` block, as a KafkaJS user writes them. Wire settings use librdkafka
// properties, which override that block and cover what it cannot express (linger, batch bytes, fetch bytes).
function createKafka(options: AdapterOptions): KafkaJS.Kafka {
  return new Kafka({
    kafkaJS: { brokers: options.brokers, clientId: options.clientId, logLevel: logLevel.NOTHING },
  });
}

function producerConfig(wire: WireSettings): KafkaJS.ProducerConstructorConfig {
  return {
    acks: wire.acks,
    'linger.ms': wire.lingerMs,
    'batch.size': wire.batchBytes,
    'compression.codec': wire.compression,
    'max.in.flight.requests.per.connection': wire.maxInFlight,
    'enable.idempotence': wire.idempotent,
  };
}

function consumerConfig(groupId: string, wire: WireSettings): KafkaJS.ConsumerConstructorConfig {
  return {
    kafkaJS: { groupId, fromBeginning: true },
    'fetch.min.bytes': wire.fetchMinBytes,
    'fetch.max.bytes': wire.fetchMaxBytes,
    'fetch.wait.max.ms': wire.fetchMaxWaitMs,
    // Two librdkafka names for one setting. The kafkaJS block always sets both (1 MiB and 10 MiB), and the
    // last one applied wins, so both are overridden.
    'max.partition.fetch.bytes': wire.partitionFetchMaxBytes,
    'fetch.message.max.bytes': wire.partitionFetchMaxBytes,
  };
}

// send() writes partition, timestamp and converted headers back onto each message object, so the shared
// record pool must never be passed in directly.
function toMessages(records: readonly BenchRecord[]): KafkaJS.Message<Buffer, Buffer>[] {
  return records.map((record) => ({ key: record.key, value: record.value, headers: record.headers }));
}

function createProducer(options: AdapterOptions): BenchProducer {
  const kafka = createKafka(options);
  const producer = kafka.producer<Buffer, Buffer>(options.profile === 'normalized' ? producerConfig(options.wire) : {});
  return {
    connect: () => producer.connect(),
    async send(topic, records) {
      await producer.send({ topic, messages: toMessages(records) });
    },
    disconnect: () => producer.disconnect(),
  };
}

function createConsumer(options: AdapterOptions): BenchConsumer {
  const kafka = createKafka(options);
  let consumer: KafkaJS.Consumer | null = null;

  return {
    async connect() {
      // The library needs the groupId to build a consumer, and that only arrives with start().
    },
    async start(consume: ConsumeOptions) {
      const isNormalized = options.profile === 'normalized';
      const hasExplicitCommits = consume.commitEvery > 0;
      const config = isNormalized
        ? consumerConfig(consume.groupId, options.wire)
        : { kafkaJS: { groupId: consume.groupId, fromBeginning: true } };
      // Explicit commits only, unless the defaults profile leaves the library's auto-commit on.
      if (isNormalized || hasExplicitCommits) {
        config['enable.auto.commit'] = false;
      }
      const activeConsumer = kafka.consumer(config);
      consumer = activeConsumer;
      await activeConsumer.connect();
      await activeConsumer.subscribe({ topics: [consume.topic] });

      const committer = hasExplicitCommits
        ? createOffsetCommitter<KafkaJS.KafkaMessage>(consume.commitEvery, (lastRecords) =>
            activeConsumer.commitOffsets(
              [...lastRecords].map(([partition, message]) => ({
                topic: consume.topic,
                partition,
                offset: nextOffsetAfter(message.offset).toString(),
              })),
            ),
          )
        : null;

      if (consume.mode === 'each-batch') {
        await activeConsumer.run({
          eachBatch: async ({ batch }: KafkaJS.EachBatchPayload) => {
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
        eachMessage: async ({ partition, message }: KafkaJS.EachMessagePayload) => {
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
  id: 'confluent',
  packageName,
  version: () => readInstalledVersion(packageName),
  // Every WireSettings field maps to a librdkafka property; `check.crcs` stays at its default (false).
  capabilityGaps: (_options, scenario) => unverifiedCrcGaps(scenario),
  createProducer,
  createConsumer,
};

export default adapter;
