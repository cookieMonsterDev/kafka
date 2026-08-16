import { describe, expect, it } from 'vitest';
import { Encoder } from '../encoder';
import { zstdCodec } from './zstd';

describe('protocol/compression/zstd', () => {
  it('round-trips arbitrary bytes', async () => {
    const encoder = new Encoder().writeString('hello kafka').writeInt32(42);
    const compressed = await zstdCodec.compress(encoder);
    const decompressed = await zstdCodec.decompress(compressed);
    expect(decompressed).toEqual(encoder.buffer);
  });

  it('actually compresses (output starts with the zstd magic number)', async () => {
    const encoder = new Encoder().writeBuffer(Buffer.alloc(1024, 'a'));
    const compressed = await zstdCodec.compress(encoder);
    // https://datatracker.ietf.org/doc/html/rfc8878#section-3.1.1 — magic number 0xFD2FB528 (little-endian bytes)
    expect(compressed.subarray(0, 4)).toEqual(Buffer.from([0x28, 0xb5, 0x2f, 0xfd]));
  });
});
