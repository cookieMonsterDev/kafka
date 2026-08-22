import type { EachBatchHandler, EachMessageHandler } from '../src/consumer/types';
import { Decoder } from '../src/protocol/decoder';
import { decodeRecordBatch, encodeRecordBatch } from '../src/protocol/records/batch';
import { encodeRecord } from '../src/protocol/records/record';
import { liveBrokers, measure, printStats, type BenchStats } from './measure';

const RECORD_COUNT = Number(process.env.BENCH_RECORDS ?? 10_000);
const VALUE = Buffer.from('0123456789abcdef0123456789abcdef');

async function handlerWork(value: Buffer | null): Promise<number> {
  if (!value) return 0;
  let hash = 0;
  for (let i = 0; i < value.length; i++) {
    hash = (hash + (value[i] ?? 0)) | 0;
  }
  return hash;
}

export async function benchConsumeHandlers(): Promise<BenchStats | null> {
  const records = new Array(RECORD_COUNT);
  for (let i = 0; i < RECORD_COUNT; i++) {
    records[i] = encodeRecord({ offsetDelta: i, key: `k-${i}`, value: VALUE });
  }
  const encoded = await encodeRecordBatch({ lastOffsetDelta: RECORD_COUNT - 1, records });
  const decoded = await decodeRecordBatch(new Decoder(encoded.buffer));
  const messages = decoded.records;

  const eachMessage = await measure({
    name: `consume eachMessage analogue (${RECORD_COUNT} records, concurrency 1, no network)`,
    iterations: 10,
    messagesPerIter: RECORD_COUNT,
    bytesPerIter: encoded.size(),
    run: async () => {
      for (const message of messages) {
        await handlerWork(message.value);
      }
    },
  });
  printStats(eachMessage);

  const eachBatch = await measure({
    name: `consume eachBatch analogue (${RECORD_COUNT} records, concurrency 1, no network)`,
    iterations: 10,
    messagesPerIter: RECORD_COUNT,
    bytesPerIter: encoded.size(),
    run: async () => {
      let hash = 0;
      for (const message of messages) {
        hash = (hash + (await handlerWork(message.value))) | 0;
      }
      if (hash === Number.MAX_SAFE_INTEGER) throw new Error('unreachable');
    },
  });
  printStats(eachBatch);

  const concurrency = 4;
  const shardSize = Math.ceil(messages.length / concurrency);
  const shards = Array.from({ length: concurrency }, (_, shard) =>
    messages.slice(shard * shardSize, (shard + 1) * shardSize),
  );

  const concurrent = await measure({
    name: `consume eachBatch analogue (${RECORD_COUNT} records, concurrency ${concurrency}, no network)`,
    iterations: 10,
    messagesPerIter: RECORD_COUNT,
    bytesPerIter: encoded.size(),
    run: async () => {
      await Promise.all(
        shards.map(async (shard) => {
          for (const message of shard) {
            await handlerWork(message.value);
          }
        }),
      );
    },
  });
  printStats(concurrent);

  const brokers = liveBrokers();
  if (!brokers) {
    console.log('skipping live eachMessage vs eachBatch (set KAFKA_EXTERNAL=1 or KAFKA_BROKERS=host:port)');
    return concurrent;
  }

  const { Kafka, logLevel } = await import('../src/index');
  const topic = process.env.BENCH_TOPIC ?? 'bench-consume-handlers';
  const kafka = new Kafka({ clientId: 'bench-consume', brokers, logLevel: logLevel.NOTHING });
  const producer = kafka.producer({ lingerMs: 5, batchSize: 16_384 });
  await producer.connect();
  try {
    const messagesToSend = Array.from({ length: RECORD_COUNT }, (_, i) => ({
      key: `k-${i}`,
      value: VALUE,
    }));
    await producer.send({ topic, messages: messagesToSend });
  } finally {
    await producer.disconnect();
  }

  async function consumeWith(
    name: string,
    runConfig: {
      eachMessage?: EachMessageHandler;
      eachBatch?: EachBatchHandler;
      partitionsConsumedConcurrently?: number;
    },
  ): Promise<BenchStats> {
    const consumer = kafka.consumer({ groupId: `bench-${name}-${Date.now()}` });
    await consumer.connect();
    await consumer.subscribe({ topics: [topic], fromBeginning: true });
    let seen = 0;
    const started = Date.now();
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`${name} timed out after 60s`)), 60_000);
      const finish = (): void => {
        clearTimeout(timer);
        resolve();
      };
      void consumer
        .run({
          partitionsConsumedConcurrently: runConfig.partitionsConsumedConcurrently ?? 1,
          eachMessage: runConfig.eachMessage
            ? async (payload) => {
                await runConfig.eachMessage?.(payload);
                seen += 1;
                if (seen >= RECORD_COUNT) finish();
              }
            : undefined,
          eachBatch: runConfig.eachBatch
            ? async (payload) => {
                await runConfig.eachBatch?.(payload);
                seen += payload.batch.messages.length;
                if (seen >= RECORD_COUNT) finish();
              }
            : undefined,
        })
        .catch(reject);
    });
    const elapsedMs = Date.now() - started;
    await consumer.stop();
    await consumer.disconnect();
    const stats: BenchStats = {
      name: `live ${name}`,
      iterations: 1,
      messages: seen,
      bytes: VALUE.length * seen,
      elapsedMs,
      msgsPerSec: elapsedMs > 0 ? seen / (elapsedMs / 1000) : 0,
      mbPerSec: elapsedMs > 0 ? (VALUE.length * seen) / (elapsedMs / 1000) / (1024 * 1024) : 0,
      p50Ms: elapsedMs,
      p99Ms: elapsedMs,
      rssMb: process.memoryUsage().rss / (1024 * 1024),
      heapUsedMb: process.memoryUsage().heapUsed / (1024 * 1024),
      gcCount: 0,
      youngGcCount: 0,
    };
    printStats(stats);
    return stats;
  }

  await consumeWith('eachMessage concurrency 1', {
    eachMessage: async ({ message }) => {
      await handlerWork(message.value);
    },
  });
  await consumeWith('eachBatch concurrency 1', {
    eachBatch: async ({ batch }) => {
      for (const message of batch.messages) {
        await handlerWork(message.value);
      }
    },
  });
  return consumeWith('eachBatch concurrency 4', {
    partitionsConsumedConcurrently: 4,
    eachBatch: async ({ batch }) => {
      for (const message of batch.messages) {
        await handlerWork(message.value);
      }
    },
  });
}
