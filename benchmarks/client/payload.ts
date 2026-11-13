import type { PayloadSize } from './spec.ts';
import type { BenchRecord } from './adapters/types.ts';

export interface PayloadShape {
  keyBytes: number;
  valueBytes: number;
  headerCount: number;
}

export const payloadShapes: Readonly<Record<PayloadSize, PayloadShape>> = {
  small: { keyBytes: 16, valueBytes: 100, headerCount: 0 },
  medium: { keyBytes: 16, valueBytes: 1024, headerCount: 2 },
};

const headerValueBytes = 16;

// Printable ASCII only, so the same bytes can be written one per line into the payload file that
// kafka-producer-perf-test.sh reads when the runner seeds a topic.
const alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';

/** mulberry32: small, fast, deterministic for a given 32-bit seed. */
export function createRandom(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function randomText(random: () => number, length: number): Buffer {
  const bytes = Buffer.allocUnsafe(length);
  for (let index = 0; index < length; index++) {
    bytes[index] = alphabet.charCodeAt(Math.floor(random() * alphabet.length));
  }
  return bytes;
}

/**
 * Builds a fixed pool of `poolSize` records from `seed`. Scenarios cycle through the pool, so
 * payload generation stays out of the measured window and every client sends identical bytes.
 */
export function createRecordPool(size: PayloadSize, seed: number, poolSize = 1024): BenchRecord[] {
  const shape = payloadShapes[size];
  const random = createRandom(seed);
  const records: BenchRecord[] = [];
  for (let index = 0; index < poolSize; index++) {
    const key = randomText(random, shape.keyBytes);
    const value = randomText(random, shape.valueBytes);
    if (shape.headerCount === 0) {
      records.push({ key, value });
      continue;
    }
    const headers: Record<string, Buffer> = {};
    for (let header = 0; header < shape.headerCount; header++) {
      headers[`h${header}`] = randomText(random, headerValueBytes);
    }
    records.push({ key, value, headers });
  }
  return records;
}

/** Newline-separated values for `kafka-producer-perf-test.sh --payload-file`. */
export function createSeedPayloadFile(size: PayloadSize, seed: number, poolSize = 1024): string {
  return createRecordPool(size, seed, poolSize)
    .map((record) => record.value.toString('latin1'))
    .join('\n');
}
