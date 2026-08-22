import { encodeRecord } from '../src/protocol/records/record';
import { encodeRecordBatch } from '../src/protocol/records/batch';
import { liveBrokers, measure, printStats, type BenchStats } from './measure';

const ENCODE_COUNT = Number(process.env.BENCH_ENCODE_MESSAGES ?? 10_000);
const MESSAGE_COUNT = Number(process.env.BENCH_PRODUCE_MESSAGES ?? 50_000);
const VALUE = Buffer.from('0123456789abcdef0123456789abcdef');
const BATCHED_SIZE = 16_384;

export async function benchProduceLinger(): Promise<BenchStats | null> {
  const linger0 = await measure({
    name: `produce encode ${ENCODE_COUNT} msgs as 1-record batches (linger 0 analogue, no network)`,
    warmup: 0,
    iterations: 1,
    messagesPerIter: ENCODE_COUNT,
    bytesPerIter: 0,
    run: async () => {
      for (let i = 0; i < ENCODE_COUNT; i++) {
        await encodeRecordBatch({
          records: [encodeRecord({ offsetDelta: 0, key: `k-${i}`, value: VALUE })],
        });
      }
    },
  });
  printStats(linger0);

  const linger5 = await measure({
    name: `produce encode ${ENCODE_COUNT} msgs in ~${BATCHED_SIZE}-record batches (linger 5 analogue, no network)`,
    warmup: 0,
    iterations: 1,
    messagesPerIter: ENCODE_COUNT,
    bytesPerIter: 0,
    run: async () => {
      for (let offset = 0; offset < ENCODE_COUNT; offset += BATCHED_SIZE) {
        const count = Math.min(BATCHED_SIZE, ENCODE_COUNT - offset);
        const records = new Array(count);
        for (let i = 0; i < count; i++) {
          records[i] = encodeRecord({ offsetDelta: i, key: `k-${offset + i}`, value: VALUE });
        }
        await encodeRecordBatch({ lastOffsetDelta: count - 1, records });
      }
    },
  });
  printStats(linger5);

  const brokers = liveBrokers();
  if (!brokers) {
    console.log('skipping live produce send() × 50k linger 0 vs 5 (set KAFKA_EXTERNAL=1 or KAFKA_BROKERS=host:port)');
    return linger5;
  }

  const { Kafka, logLevel } = await import('../src/index');
  const topic = process.env.BENCH_TOPIC ?? 'bench-produce-linger';
  const kafka = new Kafka({ clientId: 'bench-produce', brokers, logLevel: logLevel.NOTHING });

  async function sendLoop(lingerMs: number): Promise<void> {
    const producer = kafka.producer({ lingerMs, batchSize: lingerMs > 0 ? BATCHED_SIZE : undefined });
    await producer.connect();
    try {
      const pending: Promise<unknown>[] = [];
      for (let i = 0; i < MESSAGE_COUNT; i++) {
        pending.push(
          producer.send({
            topic,
            messages: [{ key: `k-${i}`, value: VALUE }],
          }),
        );
      }
      await Promise.all(pending);
      await producer.flush();
    } finally {
      await producer.disconnect();
    }
  }

  const live0 = await measure({
    name: `live send() × ${MESSAGE_COUNT}, lingerMs 0`,
    warmup: 0,
    iterations: 1,
    messagesPerIter: MESSAGE_COUNT,
    bytesPerIter: VALUE.length * MESSAGE_COUNT,
    run: () => sendLoop(0),
  });
  printStats(live0);

  const live5 = await measure({
    name: `live send() × ${MESSAGE_COUNT}, lingerMs 5`,
    warmup: 0,
    iterations: 1,
    messagesPerIter: MESSAGE_COUNT,
    bytesPerIter: VALUE.length * MESSAGE_COUNT,
    run: () => sendLoop(5),
  });
  printStats(live5);
  return live5;
}
