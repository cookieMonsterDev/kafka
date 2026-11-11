/**
 * GZIP and ZSTD: `node:zlib` (what `gzip.ts` / `zstd.ts` use) against Bun's own compression APIs.
 * Standalone so it runs unchanged on both runtimes:
 *
 *   node bench/compression-zlib.ts   # node:zlib only
 *   bun bench/compression-zlib.ts    # node:zlib and Bun.* side by side
 *
 * Columns: `ops/s` awaits one op at a time; `ops/s x8` keeps 8 ops in flight; `loop µs/op` is the
 * time the calling thread is blocked inside the call (the whole op for a `*Sync` API, only the
 * hand-off for an async one), i.e. how long the event loop cannot serve sockets or timers.
 * `BENCH_MS` sets the time spent per row (default 400).
 *
 * Outcome on Bun 1.4.x (gate: at least 10% faster at 16 KiB to 1 MiB with identical limits and errors):
 * only ZSTD compression switched, to the async `Bun.zstdCompress` (`zstd.ts`). Bun's decompress APIs
 * take no output limit, so they cannot enforce `MAX_DECOMPRESSED_SIZE`, and its GZIP APIs are
 * synchronous: they block the event loop and lose to `node:zlib` once several batches are in flight.
 */
import { performance } from 'node:perf_hooks';
import { promisify } from 'node:util';
import { gzip, unzip, zstdCompress, zstdDecompress } from 'node:zlib';

type Bytes = Uint8Array;

/** The subset of `globalThis.Bun` this bench calls; `@types/bun` is not a dependency. */
interface BunCompression {
  gzipSync(input: Bytes): Bytes;
  gunzipSync(input: Bytes): Bytes;
  zstdCompressSync(input: Bytes): Bytes;
  zstdDecompressSync(input: Bytes): Bytes;
  zstdCompress(input: Bytes): Promise<Bytes>;
  zstdDecompress(input: Bytes): Promise<Bytes>;
}

interface Variant {
  name: string;
  run: (input: Bytes) => Bytes | Promise<Bytes>;
}

const BENCH_MS = Number(process.env.BENCH_MS ?? 400);
const SIZES = [1024, 16 * 1024, 64 * 1024, 1024 * 1024];
const CONCURRENCY = 8;

const nodeGzip = promisify(gzip);
const nodeUnzip = promisify(unzip);
const nodeZstdCompress = promisify(zstdCompress);
const nodeZstdDecompress = promisify(zstdDecompress);

/** Record-batch-like text: repeated field names with varying ids and values, so ratios are realistic. */
function payload(size: number): Buffer {
  const parts: string[] = [];
  let length = 0;
  for (let i = 0; length < size; i++) {
    const record = `{"id":${i},"user":"user-${(i * 7919) % 1000}","event":"page_view","ts":${1_700_000_000_000 + i * 37},"value":${((i * 2654435761) >>> 0) % 100000}}`;
    parts.push(record);
    length += record.length;
  }
  return Buffer.from(parts.join('')).subarray(0, size);
}

async function timeSequential(variant: Variant, input: Bytes): Promise<{ opsPerSec: number; loopUs: number }> {
  let ops = 0;
  let blockedMs = 0;
  const started = performance.now();
  while (performance.now() - started < BENCH_MS) {
    const t0 = performance.now();
    const result = variant.run(input);
    blockedMs += performance.now() - t0;
    await result;
    ops++;
  }
  const elapsedMs = performance.now() - started;
  return { opsPerSec: (ops / elapsedMs) * 1000, loopUs: (blockedMs / ops) * 1000 };
}

async function timeConcurrent(variant: Variant, input: Bytes): Promise<number> {
  let ops = 0;
  const started = performance.now();
  const lane = async (): Promise<void> => {
    while (performance.now() - started < BENCH_MS) {
      await variant.run(input);
      ops++;
    }
  };
  await Promise.all(Array.from({ length: CONCURRENCY }, lane));
  return (ops / (performance.now() - started)) * 1000;
}

function variants(
  bun: BunCompression | undefined,
): Record<string, { input: (raw: Buffer) => Promise<Bytes>; list: Variant[] }> {
  const gzipCompress: Variant[] = [{ name: 'node:zlib gzip', run: (b) => nodeGzip(b) }];
  const gzipDecompress: Variant[] = [{ name: 'node:zlib unzip', run: (b) => nodeUnzip(b) }];
  const zstdCompressList: Variant[] = [{ name: 'node:zlib zstdCompress', run: (b) => nodeZstdCompress(b) }];
  const zstdDecompressList: Variant[] = [{ name: 'node:zlib zstdDecompress', run: (b) => nodeZstdDecompress(b) }];
  if (bun) {
    gzipCompress.push({ name: 'Bun.gzipSync', run: (b) => bun.gzipSync(b) });
    gzipDecompress.push({ name: 'Bun.gunzipSync', run: (b) => bun.gunzipSync(b) });
    zstdCompressList.push(
      { name: 'Bun.zstdCompress', run: (b) => bun.zstdCompress(b) },
      { name: 'Bun.zstdCompressSync', run: (b) => bun.zstdCompressSync(b) },
    );
    zstdDecompressList.push(
      { name: 'Bun.zstdDecompress', run: (b) => bun.zstdDecompress(b) },
      { name: 'Bun.zstdDecompressSync', run: (b) => bun.zstdDecompressSync(b) },
    );
  }
  return {
    'gzip compress': { input: async (raw) => raw, list: gzipCompress },
    'gzip decompress': { input: (raw) => nodeGzip(raw), list: gzipDecompress },
    'zstd compress': { input: async (raw) => raw, list: zstdCompressList },
    'zstd decompress': { input: (raw) => nodeZstdCompress(raw), list: zstdDecompressList },
  };
}

export async function benchCompressionZlib(): Promise<void> {
  const bun = (globalThis as { Bun?: BunCompression }).Bun;
  const runtime = process.versions.bun ? `bun ${process.versions.bun}` : `node ${process.versions.node}`;
  console.log(`compression: node:zlib vs Bun APIs (${runtime}, ${BENCH_MS} ms per row)`);
  console.log('case                    size      variant                     ops/s   ops/s x8   loop µs/op');

  for (const [label, { input, list }] of Object.entries(variants(bun))) {
    for (const size of SIZES) {
      const data = await input(payload(size));
      for (const variant of list) {
        await timeSequential(variant, data); // warm-up
        const { opsPerSec, loopUs } = await timeSequential(variant, data);
        const concurrent = await timeConcurrent(variant, data);
        console.log(
          `${label.padEnd(24)}${`${size / 1024} KiB`.padEnd(10)}${variant.name.padEnd(26)}` +
            `${opsPerSec.toFixed(0).padStart(8)}${concurrent.toFixed(0).padStart(11)}${loopUs.toFixed(1).padStart(13)}`,
        );
      }
    }
  }
}

const isDirect = process.argv[1] !== undefined && process.argv[1].includes('compression-zlib');
if (isDirect) {
  benchCompressionZlib().catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  });
}
