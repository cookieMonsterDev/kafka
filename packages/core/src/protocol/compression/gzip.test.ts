import { describe, expect, it } from 'vitest';
import { KafkaNonRetriableError } from '../../errors';
import { Encoder } from '../encoder';
import { gzipCodec, decompressGzip } from './gzip';

describe('protocol/compression/gzip', () => {
  it('round-trips arbitrary bytes', async () => {
    const encoder = new Encoder().writeString('hello kafka').writeInt32(42);
    const compressed = await gzipCodec.compress(encoder);
    const decompressed = await gzipCodec.decompress(compressed);
    expect(decompressed).toEqual(encoder.buffer);
  });

  it('actually compresses (output looks like gzip, not raw passthrough)', async () => {
    const encoder = new Encoder().writeBuffer(Buffer.alloc(1024, 'a'));
    const compressed = await gzipCodec.compress(encoder);
    expect(compressed[0]).toBe(0x1f);
    expect(compressed[1]).toBe(0x8b);
  });

  it('rejects decompressed output that exceeds the size cap', async () => {
    const encoder = new Encoder().writeBuffer(Buffer.alloc(64, 'a'));
    const compressed = await gzipCodec.compress(encoder);
    await expect(decompressGzip(compressed, 1)).rejects.toBeInstanceOf(KafkaNonRetriableError);
  });

  it('propagates zlib errors that are not a size-cap violation', async () => {
    await expect(decompressGzip(Buffer.from('not gzip'))).rejects.toThrow();
    await expect(decompressGzip(Buffer.from('not gzip'))).rejects.not.toBeInstanceOf(KafkaNonRetriableError);
  });

  it('honors compressionLevel: level 1 produces a larger output than level 9 for compressible data', async () => {
    const encoder = new Encoder().writeBuffer(Buffer.from('kafka-kafka-kafka-'.repeat(200)));
    const fast = await gzipCodec.compress(encoder, 1);
    const best = await gzipCodec.compress(encoder, 9);

    expect(fast.length).toBeGreaterThan(best.length);
    expect(await gzipCodec.decompress(fast)).toEqual(encoder.buffer);
    expect(await gzipCodec.decompress(best)).toEqual(encoder.buffer);
  });

  it('omits the level option entirely when compressionLevel is not set', async () => {
    const encoder = new Encoder().writeString('hello kafka');
    // No level passed - falls back to zlib's own default, still round-trips correctly.
    const compressed = await gzipCodec.compress(encoder, undefined);
    expect(await gzipCodec.decompress(compressed)).toEqual(encoder.buffer);
  });
});
