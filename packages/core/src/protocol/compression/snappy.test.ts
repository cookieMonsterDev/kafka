import { inspect } from 'node:util';
import { compress as snappyCompress } from 'snappyjs';
import { describe, expect, it } from 'vitest';
import { KafkaNonRetriableError } from '../../errors';
import { Encoder } from '../encoder';
import { compressSnappySync } from './codec-worker';
import { decompressSnappy, snappyCodec } from './snappy';

const XERIAL_MAGIC = Buffer.from([0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00]);

describe('protocol/compression/snappy', () => {
  it('round-trips arbitrary bytes', async () => {
    const encoder = new Encoder().writeString('hello kafka').writeInt32(42);
    const compressed = await snappyCodec.compress(encoder);
    const decompressed = await snappyCodec.decompress(compressed);
    expect(decompressed).toEqual(encoder.buffer);
  });

  it('round-trips payloads larger than the xerial 32KiB block size', async () => {
    const encoder = new Encoder().writeBuffer(Buffer.alloc(40 * 1024, 'a'));
    const compressed = await snappyCodec.compress(encoder);
    const decompressed = await snappyCodec.decompress(compressed);
    expect(decompressed).toEqual(encoder.buffer);
  });

  it('writes xerial snappy-java framing (magic header)', async () => {
    const encoder = new Encoder().writeBuffer(Buffer.alloc(1024, 'a'));
    const compressed = await snappyCodec.compress(encoder);
    expect(compressed.subarray(0, 8)).toEqual(XERIAL_MAGIC);
  });

  it('decompresses raw snappy blocks (unframed producers)', async () => {
    const raw = Buffer.from('raw snappy payload');
    const compressed = Buffer.from(snappyCompress(raw));
    const decompressed = await snappyCodec.decompress(compressed);
    expect(decompressed).toEqual(raw);
  });

  it('decompresses xerial-framed payloads produced by this codec', async () => {
    const encoder = new Encoder().writeString('xerial');
    const compressed = await snappyCodec.compress(encoder);
    expect(await decompressSnappy(compressed)).toEqual(encoder.buffer);
  });

  it('rejects decompressed output that exceeds the size cap', async () => {
    const encoder = new Encoder().writeBuffer(Buffer.alloc(64, 'a'));
    const compressed = await snappyCodec.compress(encoder);
    await expect(decompressSnappy(compressed, 1)).rejects.toBeInstanceOf(KafkaNonRetriableError);
  });

  it('rejects raw snappy that exceeds the size cap', async () => {
    const raw = Buffer.alloc(64, 'b');
    const compressed = Buffer.from(snappyCompress(raw));
    await expect(decompressSnappy(compressed, 1)).rejects.toBeInstanceOf(KafkaNonRetriableError);
  });

  it('propagates snappy errors that are not a size-cap violation', async () => {
    await expect(decompressSnappy(Buffer.from('not snappy'))).rejects.toThrow();
    await expect(decompressSnappy(Buffer.from('not snappy'))).rejects.not.toBeInstanceOf(KafkaNonRetriableError);
  });

  it('does not resolve compress on the calling tick (off-thread or native async)', () => {
    const encoder = new Encoder().writeBuffer(Buffer.alloc(8 * 1024, 'a'));
    const pending = snappyCodec.compress(encoder);
    expect(inspect(pending)).toContain('pending');
    return pending;
  });

  it('round-trips concurrent compress/decompress without mixing payloads', async () => {
    const payloads = ['alpha', 'bravo', 'charlie', 'delta'].map((value) => new Encoder().writeString(value));
    const compressed = await Promise.all(payloads.map((encoder) => snappyCodec.compress(encoder)));
    const decompressed = await Promise.all(compressed.map((buf) => snappyCodec.decompress(buf)));
    expect(decompressed.map((buf) => buf.toString())).toEqual(payloads.map((encoder) => encoder.buffer.toString()));
  });

  it('JS helper writes the same xerial framing the codec consumes', async () => {
    const raw = Buffer.from('sync helper');
    const compressed = compressSnappySync(raw);
    expect(compressed.subarray(0, 8)).toEqual(XERIAL_MAGIC);
    expect(await decompressSnappy(compressed)).toEqual(raw);
  });
});
