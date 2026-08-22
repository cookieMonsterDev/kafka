import { encodeRecord } from '../src/protocol/records/record';
import { encodeRecordBatch } from '../src/protocol/records/batch';
import { measure, printStats, type BenchStats } from './measure';

const RECORD_COUNT = Number(process.env.BENCH_RECORDS ?? 10_000);
const ITERATIONS = Number(process.env.BENCH_ITERS ?? 20);
const VALUE = Buffer.from('0123456789abcdef0123456789abcdef');

export async function benchEncodeRecordBatch(): Promise<BenchStats> {
  let lastBytes = 0;

  const stats = await measure({
    name: `encode ${RECORD_COUNT} records → RecordBatch (no network)`,
    iterations: ITERATIONS,
    messagesPerIter: RECORD_COUNT,
    bytesPerIter: 0,
    run: async () => {
      const records = new Array(RECORD_COUNT);
      for (let i = 0; i < RECORD_COUNT; i++) {
        records[i] = encodeRecord({
          offsetDelta: i,
          key: `k-${i}`,
          value: VALUE,
        });
      }
      const batch = await encodeRecordBatch({
        lastOffsetDelta: RECORD_COUNT - 1,
        records,
      });
      lastBytes = batch.size();
    },
  });

  stats.bytes = lastBytes * ITERATIONS;
  stats.mbPerSec = stats.elapsedMs > 0 ? stats.bytes / (stats.elapsedMs / 1000) / (1024 * 1024) : 0;
  printStats(stats);
  return stats;
}
