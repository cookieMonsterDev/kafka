import { Decoder } from '../src/protocol/decoder';
import { decodeRecordBatch, encodeRecordBatch } from '../src/protocol/records/batch';
import { encodeRecord } from '../src/protocol/records/record';
import { measure, printStats, type BenchStats } from './measure';

const RECORD_COUNT = Number(process.env.BENCH_RECORDS ?? 20_000);
const ITERATIONS = Number(process.env.BENCH_ITERS ?? 20);
const VALUE = Buffer.from('0123456789abcdef0123456789abcdef');

async function buildBatch(): Promise<Buffer> {
  const records = new Array(RECORD_COUNT);
  for (let i = 0; i < RECORD_COUNT; i++) {
    records[i] = encodeRecord({
      offsetDelta: i,
      key: `k-${i}`,
      value: VALUE,
      headers: { 'trace-id': `trace-${i}`, source: 'bench' },
    });
  }
  const batch = await encodeRecordBatch({ lastOffsetDelta: RECORD_COUNT - 1, records });
  return batch.buffer;
}

/**
 * P2-04's "done when": an `eachBatch` consumer that only reads offsets (e.g. `lastOffset()`)
 * shouldn't pay for decoding every record's value/headers. Compares that against a consumer
 * that reads every value and header — the case decode used to pay for unconditionally.
 */
export async function benchLazyRecordDecode(): Promise<BenchStats> {
  const batch = await buildBatch();

  const offsetsOnly = await measure({
    name: `decode + read only offsets of ${RECORD_COUNT} records (lastOffset()-only eachBatch)`,
    iterations: ITERATIONS,
    messagesPerIter: RECORD_COUNT,
    bytesPerIter: batch.length,
    run: async () => {
      const decoded = await decodeRecordBatch(new Decoder(batch));
      let lastOffset = -1n;
      for (const record of decoded.records) {
        lastOffset = record.offset;
      }
      if (lastOffset !== BigInt(RECORD_COUNT - 1)) {
        throw new Error(`expected last offset ${RECORD_COUNT - 1}, got ${lastOffset}`);
      }
    },
  });
  printStats(offsetsOnly);

  const fullyConsumed = await measure({
    name: `decode + read value/headers of ${RECORD_COUNT} records (full eachMessage consumption)`,
    iterations: ITERATIONS,
    messagesPerIter: RECORD_COUNT,
    bytesPerIter: batch.length,
    run: async () => {
      const decoded = await decodeRecordBatch(new Decoder(batch));
      let bytes = 0;
      for (const record of decoded.records) {
        bytes += (record.value?.byteLength ?? 0) + Object.keys(record.headers).length;
      }
      if (bytes <= 0) {
        throw new Error('expected to read at least one byte of value across all records');
      }
    },
  });
  printStats(fullyConsumed);

  return offsetsOnly;
}
