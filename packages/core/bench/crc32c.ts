// CRC32C throughput: the slicing-by-16 `crc32cJs`, the public `crc32c` (OpenSSL when available),
// and the byte-at-a-time loop it replaced. Runs directly on both runtimes:
//
//   node bench/crc32c.ts
//   bun bench/crc32c.ts
//
// Outcome (gate: at least 15% faster at 16 KB to 1 MB on one runtime, not slower on the other): slicing-by-16 runs at
// about 2.8 GB/s against about 0.59 GB/s for the byte-at-a-time loop on Node.js 24 and 26 and Bun 1.4.0 and 1.4.4
// (macOS arm64). Neither runtime ships `crypto.hash('crc32c')`, and Bun has no CRC32C API (`Bun.hash.crc32` is IEEE).

import { randomBytes } from 'node:crypto';

type Crc32cModule = typeof import('../src/protocol/crc32c');

const SIZES = [256, 1024, 16 * 1024, 1024 * 1024];
const ROUNDS = 5;
const MIN_ROUND_MS = Number(process.env.BENCH_CRC32C_MS ?? 200);

const CASTAGNOLI_POLYNOMIAL = 0x82f63b78;
const BYTEWISE_TABLE = new Int32Array(256);
for (let byte = 0; byte < 256; byte++) {
  let crc = byte;
  for (let bit = 0; bit < 8; bit++) {
    crc = (crc & 1) === 1 ? (crc >>> 1) ^ CASTAGNOLI_POLYNOMIAL : crc >>> 1;
  }
  BYTEWISE_TABLE[byte] = crc;
}

/** The single-table loop `crc32cJs` used before slicing-by-16, kept as the baseline. */
function crc32cBytewise(buffer: Buffer): number {
  let crc = -1;
  for (let i = 0; i < buffer.length; i++) {
    crc = (BYTEWISE_TABLE[(crc ^ (buffer[i] as number)) & 0xff] as number) ^ (crc >>> 8);
  }
  return (crc ^ -1) >>> 0;
}

/**
 * Median throughput in MB/s over `ROUNDS` timed rounds of at least `MIN_ROUND_MS` each. Returns the
 * last checksum too, so every call's result is used and the caller can verify it.
 */
function measureThroughput(
  checksum: (buffer: Buffer) => number,
  buffer: Buffer,
): { mbPerSec: number; lastChecksum: number } {
  let lastChecksum = 0;
  for (let i = 0; i < 50; i++) lastChecksum = checksum(buffer);

  const nsPerCall: number[] = [];
  for (let round = 0; round < ROUNDS; round++) {
    let calls = 0;
    const start = performance.now();
    let elapsedMs = 0;
    while (elapsedMs < MIN_ROUND_MS) {
      for (let i = 0; i < 20; i++) lastChecksum = checksum(buffer);
      calls += 20;
      elapsedMs = performance.now() - start;
    }
    nsPerCall.push((elapsedMs * 1e6) / calls);
  }

  nsPerCall.sort((a, b) => a - b);
  const medianNs = nsPerCall[Math.floor(ROUNDS / 2)] as number;
  return { mbPerSec: (buffer.length * 1e3) / medianNs / 1.048576, lastChecksum };
}

function formatSize(bytes: number): string {
  if (bytes >= 1024 * 1024) return `${bytes / (1024 * 1024)} MB`;
  if (bytes >= 1024) return `${bytes / 1024} KB`;
  return `${bytes} B`;
}

export async function benchCrc32c(): Promise<void> {
  // A `.ts` URL rather than a static import, so `node bench/crc32c.ts` resolves it without a loader.
  const { crc32c, crc32cJs, usesNativeCrc32c } = (await import(
    new URL('../src/protocol/crc32c.ts', import.meta.url).href
  )) as Crc32cModule;

  const runtime = process.versions.bun === undefined ? `Node ${process.version}` : `Bun ${process.versions.bun}`;
  console.log(`CRC32C throughput, ${runtime}, MB/s (median of ${ROUNDS}); public crc32c native: ${usesNativeCrc32c}`);

  const implementations: Record<string, (buffer: Buffer) => number> = {
    'byte-at-a-time': crc32cBytewise,
    'crc32cJs (slicing-by-16)': crc32cJs,
    'crc32c (public)': crc32c,
  };

  console.log(['size', ...Object.keys(implementations)].map((label) => label.padStart(26)).join(''));
  for (const size of SIZES) {
    const buffer = randomBytes(size);
    const expected = crc32cBytewise(buffer);
    const row = [formatSize(size).padStart(26)];
    for (const [name, checksum] of Object.entries(implementations)) {
      const { mbPerSec, lastChecksum } = measureThroughput(checksum, buffer);
      if (lastChecksum !== expected) throw new Error(`${name} disagrees with the baseline at ${size} bytes`);
      row.push(mbPerSec.toFixed(0).padStart(26));
    }
    console.log(row.join(''));
  }
}

const isDirect = process.argv[1] !== undefined && process.argv[1].includes('bench/crc32c');
if (isDirect) {
  benchCrc32c().catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  });
}
