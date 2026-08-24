import { Decoder } from '../src/protocol/decoder';
import { Encoder } from '../src/protocol/encoder';
import { encodeRecord } from '../src/protocol/records/record';
import { decodeRecordBatch, encodeRecordBatch } from '../src/protocol/records/batch';
import { fetchResponseV4 } from '../src/protocol/requests/fetch/v4/response';
import { measure, printStats, type BenchStats } from './measure';

const RECORD_COUNT = Number(process.env.BENCH_RECORDS ?? 10_000);
const ITERATIONS = Number(process.env.BENCH_ITERS ?? 20);
const VALUE = Buffer.from('0123456789abcdef0123456789abcdef');

async function buildFetchV4Payload(): Promise<{ payload: Buffer; batch: Buffer }> {
  const records = new Array(RECORD_COUNT);
  for (let i = 0; i < RECORD_COUNT; i++) {
    records[i] = encodeRecord({ offsetDelta: i, key: `k-${i}`, value: VALUE });
  }
  const batch = await encodeRecordBatch({ lastOffsetDelta: RECORD_COUNT - 1, records });

  const partition = new Encoder()
    .writeInt32(0)
    .writeInt16(0)
    .writeInt64(BigInt(RECORD_COUNT))
    .writeInt64(0n)
    .writeArray([])
    .writeInt32(batch.size())
    .writeEncoder(batch);

  const topic = new Encoder().writeString('bench-topic').writeArray([partition], 'object');
  const payload = new Encoder().writeInt32(0).writeArray([topic], 'object').buffer;

  return { payload, batch: batch.buffer };
}

export async function benchDecodeFetchPayload(): Promise<BenchStats> {
  const { payload, batch } = await buildFetchV4Payload();

  const decodeBatch = await measure({
    name: `decode RecordBatch of ${RECORD_COUNT} records`,
    iterations: ITERATIONS,
    messagesPerIter: RECORD_COUNT,
    bytesPerIter: batch.length,
    run: async () => {
      const decoded = await decodeRecordBatch(new Decoder(batch));
      if (decoded.records.length !== RECORD_COUNT) {
        throw new Error(`expected ${RECORD_COUNT} records, got ${decoded.records.length}`);
      }
    },
  });
  printStats(decodeBatch);

  const decodeFetch = await measure({
    name: `decode Fetch v4 payload of ${RECORD_COUNT} records`,
    iterations: ITERATIONS,
    messagesPerIter: RECORD_COUNT,
    bytesPerIter: payload.length,
    run: async () => {
      const decoded = await fetchResponseV4().decode(payload);
      const count = decoded.responses[0]?.partitions[0]?.messages.length ?? 0;
      if (count !== RECORD_COUNT) {
        throw new Error(`expected ${RECORD_COUNT} fetch records, got ${count}`);
      }
    },
  });
  printStats(decodeFetch);

  return decodeFetch;
}
