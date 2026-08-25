import { beforeEach, describe, expect, it } from 'vitest';
import { acquireBuffer, bufferPoolStats, clearBufferPool, releaseBuffer } from './buffer-pool';

describe('protocol/buffer-pool', () => {
  beforeEach(() => {
    clearBufferPool();
  });

  it('allocates on a cold pool', () => {
    acquireBuffer(512);
    expect(bufferPoolStats()).toEqual({ acquireCount: 1, allocCount: 1, releaseCount: 0 });
  });

  it('reuses a released buffer of the same size instead of allocating again', () => {
    const first = acquireBuffer(512);
    releaseBuffer(first);
    const second = acquireBuffer(512);

    expect(second).toBe(first);
    expect(bufferPoolStats()).toEqual({ acquireCount: 2, allocCount: 1, releaseCount: 1 });
  });

  it('keeps size classes separate', () => {
    const small = acquireBuffer(512);
    releaseBuffer(small);
    acquireBuffer(1024);

    expect(bufferPoolStats()).toEqual({ acquireCount: 2, allocCount: 2, releaseCount: 1 });
  });

  it('does not pool buffers larger than the max pooled size', () => {
    const huge = acquireBuffer(1 << 20);
    releaseBuffer(huge);
    acquireBuffer(1 << 20);

    expect(bufferPoolStats()).toEqual({ acquireCount: 2, allocCount: 2, releaseCount: 1 });
  });

  it('caps how many buffers a single size class retains', () => {
    const size = 256;
    const released: Buffer[] = [];
    for (let i = 0; i < 32; i++) {
      released.push(acquireBuffer(size));
    }
    for (const buffer of released) {
      releaseBuffer(buffer);
    }

    // Only MAX_FREE_PER_BUCKET (16) of the 32 released buffers can have been kept, so half of
    // the next 32 acquisitions must fall back to a fresh allocation.
    for (let i = 0; i < 32; i++) {
      acquireBuffer(size);
    }

    const stats = bufferPoolStats();
    expect(stats.acquireCount).toBe(64);
    expect(stats.allocCount).toBe(32 + 16);
  });

  it('ignores a zero-length buffer on release', () => {
    releaseBuffer(Buffer.alloc(0));
    acquireBuffer(0);
    expect(bufferPoolStats()).toEqual({ acquireCount: 1, allocCount: 1, releaseCount: 1 });
  });
});
