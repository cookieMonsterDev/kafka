/**
 * Small size-classed free list for `Encoder`'s backing buffers. Every `Encoder` size is already
 * rounded up to a power of two (`Encoder.nextPowerOfTwo`), so buckets can key on exact length
 * with no rounding of their own.
 *
 * Buffers above `MAX_POOLED_SIZE` are one-off (a single huge record batch, say) and not worth
 * retaining; each bucket is capped so a burst of unusually large requests can't pin memory
 * forever once traffic drops back down.
 */
const MAX_POOLED_SIZE = 1 << 18; // 256 KiB
const MAX_FREE_PER_BUCKET = 16;

const freeLists = new Map<number, Buffer[]>();

let acquireCount = 0;
let allocCount = 0;
let releaseCount = 0;

export function acquireBuffer(size: number): Buffer {
  acquireCount += 1;

  const bucket = freeLists.get(size);
  const pooled = bucket?.pop();
  if (pooled !== undefined) {
    return pooled;
  }

  allocCount += 1;
  return Buffer.allocUnsafe(size);
}

export function releaseBuffer(buffer: Buffer): void {
  releaseCount += 1;

  const size = buffer.length;
  if (size === 0 || size > MAX_POOLED_SIZE) {
    return;
  }

  let bucket = freeLists.get(size);
  if (bucket === undefined) {
    bucket = [];
    freeLists.set(size, bucket);
  }

  if (bucket.length < MAX_FREE_PER_BUCKET) {
    bucket.push(buffer);
  }
}

export interface BufferPoolStats {
  /** How many buffers were requested from the pool. */
  acquireCount: number;
  /** How many of those requests missed the free list and hit `Buffer.allocUnsafe`. */
  allocCount: number;
  /** How many buffers were returned to the pool (including ones too large to keep). */
  releaseCount: number;
}

export function bufferPoolStats(): BufferPoolStats {
  return { acquireCount, allocCount, releaseCount };
}

export function resetBufferPoolStats(): void {
  acquireCount = 0;
  allocCount = 0;
  releaseCount = 0;
}

/** Test/bench-only escape hatch: drop every pooled buffer so stats reflect a clean pool. */
export function clearBufferPool(): void {
  freeLists.clear();
  resetBufferPoolStats();
}
