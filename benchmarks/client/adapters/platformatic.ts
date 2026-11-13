import { Consumer, MessagesStreamModes, Producer, stringSerializer } from '@platformatic/kafka';
import type {
  ConsumerOptions,
  Message,
  MessagesStream,
  MessageToProduce,
  ProducerOptions,
  SendOptions,
} from '@platformatic/kafka';
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
  unverifiedCrcGaps,
} from './shared.ts';

const packageName = clientPackageNames.platformatic;

// Header keys arrive as strings like in every other client; the library's own string serializer turns them
// into the Buffer the wire format needs. Keys, values and header values stay Buffers with no serializer.
type HeaderKey = string;
type BenchProducerOptions = ProducerOptions<Buffer, Buffer, HeaderKey, Buffer>;
type BenchSendSettings = Pick<SendOptions<Buffer, Buffer, HeaderKey, Buffer>, 'acks' | 'compression'>;
type BenchMessage = Message<Buffer, Buffer, Buffer, Buffer>;

function producerConfig(wire: WireSettings): Partial<BenchProducerOptions> {
  // The library forces maxInflights to 1 when idempotent is set; capabilityGaps reports it.
  return { idempotent: wire.idempotent, maxInflights: wire.maxInFlight };
}

// send() reads acks only from its own options (default: leader only), never from the constructor.
function sendSettings(wire: WireSettings): BenchSendSettings {
  return { acks: wire.acks, compression: wire.compression };
}

function consumerConfig(wire: WireSettings): Partial<ConsumerOptions<Buffer, Buffer, Buffer, Buffer>> {
  return {
    minBytes: wire.fetchMinBytes,
    maxBytes: wire.fetchMaxBytes,
    maxWaitTime: wire.fetchMaxWaitMs,
    maxBytesPerPartition: wire.partitionFetchMaxBytes,
  };
}

function toMessages(
  topic: string,
  records: readonly BenchRecord[],
): MessageToProduce<Buffer, Buffer, HeaderKey, Buffer>[] {
  return records.map((record) => ({ topic, key: record.key, value: record.value, headers: record.headers }));
}

function createProducer(options: AdapterOptions): BenchProducer {
  const isNormalized = options.profile === 'normalized';
  const producer = new Producer<Buffer, Buffer, HeaderKey, Buffer>({
    clientId: options.clientId,
    bootstrapBrokers: options.brokers,
    serializers: { headerKey: stringSerializer },
    ...(isNormalized ? producerConfig(options.wire) : {}),
  });
  const settings: BenchSendSettings = isNormalized ? sendSettings(options.wire) : {};
  return {
    async connect() {
      // The client connects lazily; an empty metadata request opens the bootstrap connection now.
      await producer.metadata({ topics: [] });
    },
    async send(topic, records) {
      // send() mutates its options object, so each call gets a fresh one.
      await producer.send({ messages: toMessages(topic, records), ...settings });
    },
    disconnect: () => producer.close(),
  };
}

function valueLength(message: BenchMessage): number {
  return message.value === undefined || message.value === null ? 0 : message.value.length;
}

/**
 * The stream has no handler to await, so a due commit pauses delivery until the broker answers, which is
 * what an awaited commit inside a kafkajs-style handler does.
 */
function commitWhilePaused(stream: MessagesStream<Buffer, Buffer, Buffer, Buffer>, commit: Promise<void>): void {
  stream.pause();
  commit.then(
    () => stream.resume(),
    (error: unknown) => stream.destroy(error instanceof Error ? error : new Error(String(error))),
  );
}

function createConsumer(options: AdapterOptions): BenchConsumer {
  let consumer: Consumer<Buffer, Buffer, Buffer, Buffer> | null = null;
  let streamError: Error | null = null;

  return {
    async connect() {
      // The library needs the groupId to build a consumer, and that only arrives with start().
    },
    async start(consume: ConsumeOptions) {
      const isNormalized = options.profile === 'normalized';
      const hasExplicitCommits = consume.commitEvery > 0;
      const activeConsumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
        clientId: options.clientId,
        bootstrapBrokers: options.brokers,
        groupId: consume.groupId,
        ...(isNormalized ? consumerConfig(options.wire) : {}),
        // Explicit commits only, unless the defaults profile leaves the library's auto-commit on.
        ...(isNormalized || hasExplicitCommits ? { autocommit: false } : {}),
      });
      consumer = activeConsumer;

      const stream = await activeConsumer.consume({ topics: [consume.topic], mode: MessagesStreamModes.EARLIEST });
      stream.on('error', (error) => {
        streamError = error;
      });
      const committer = hasExplicitCommits
        ? createOffsetCommitter<BenchMessage>(consume.commitEvery, (lastRecords) =>
            activeConsumer.commit({
              offsets: [...lastRecords.values()].map((message) => ({
                topic: message.topic,
                partition: message.partition,
                offset: nextOffsetAfter(message.offset),
                leaderEpoch: message.leaderEpoch,
              })),
            }),
          )
        : null;

      if (consume.mode === 'each-batch') {
        // No batch API: a "batch" is everything the stream has buffered when it signals 'readable', which is
        // normally the records of one fetch response.
        stream.on('readable', () => {
          let count = 0;
          let valueBytes = 0;
          let message = stream.read() as BenchMessage | null;
          while (message !== null) {
            count++;
            valueBytes += valueLength(message);
            committer?.seen(message.partition, message);
            message = stream.read() as BenchMessage | null;
          }
          if (count === 0) {
            return;
          }
          consume.onBatch(count, valueBytes);
          const commit = committer?.delivered(count) ?? null;
          if (commit !== null) {
            commitWhilePaused(stream, commit);
          }
        });
        return;
      }

      stream.on('data', (message: BenchMessage) => {
        consume.onMessage(message.value ?? null);
        committer?.seen(message.partition, message);
        const commit = committer?.delivered(1) ?? null;
        if (commit !== null) {
          commitWhilePaused(stream, commit);
        }
      });
    },
    async disconnect() {
      // Forced, because a non-forced close refuses to run while a consume stream is open.
      await consumer?.close(true);
      if (streamError !== null) {
        throw new Error(`platformatic consume stream failed: ${streamError.message}`, { cause: streamError });
      }
    },
  };
}

function capabilityGaps(options: AdapterOptions, scenario: ScenarioDefinition): string[] {
  const gaps = unverifiedCrcGaps(scenario);
  // Not tied to a wire setting, so reported under both profiles.
  if (scenario.kind === 'consume' && scenario.consumeMode === 'each-batch') {
    gaps.push(
      "each-batch: no batch API; onBatch receives the records buffered in the consume stream at each 'readable' event",
    );
  }
  if (options.profile !== 'normalized' || scenario.kind === 'consume') {
    return gaps;
  }
  gaps.push(...perSendBatchingGaps(options.wire));
  if (options.wire.idempotent && options.wire.maxInFlight > 1) {
    gaps.push(
      `maxInFlight: the library forces 1 in-flight request for idempotent producers, not ${options.wire.maxInFlight}`,
    );
  }
  return gaps;
}

export const adapter: ClientAdapter = {
  id: 'platformatic',
  packageName,
  version: () => readInstalledVersion(packageName),
  capabilityGaps,
  createProducer,
  createConsumer,
};

export default adapter;
