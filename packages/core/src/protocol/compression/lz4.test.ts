import { inspect } from 'node:util';
import { describe, expect, it } from 'vitest';
import { KafkaNonRetriableError } from '../../errors';
import { Encoder } from '../encoder';
import { decompressLz4, lz4Codec } from './lz4';

/** LZ4 Frame magic 0x184D2204, little-endian. @see https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md */
const LZ4_FRAME_MAGIC = Buffer.from([0x04, 0x22, 0x4d, 0x18]);
const FLG_VERSION = 0x40;
const FLG_BLOCK_INDEPENDENCE = 0x20;

describe('protocol/compression/lz4', () => {
  it('round-trips arbitrary bytes', async () => {
    const encoder = new Encoder().writeString('hello kafka').writeInt32(42);
    const compressed = await lz4Codec.compress(encoder);
    const decompressed = await lz4Codec.decompress(compressed);
    expect(decompressed).toEqual(encoder.buffer);
  });

  it('actually compresses (output starts with the LZ4 Frame magic number)', async () => {
    const encoder = new Encoder().writeBuffer(Buffer.alloc(1024, 'a'));
    const compressed = await lz4Codec.compress(encoder);
    expect(compressed.subarray(0, 4)).toEqual(LZ4_FRAME_MAGIC);
    expect((compressed[4] ?? 0) & FLG_VERSION).toBe(FLG_VERSION);
    expect((compressed[4] ?? 0) & FLG_BLOCK_INDEPENDENCE).toBe(FLG_BLOCK_INDEPENDENCE);
  });

  it('rejects decompressed output that exceeds the size cap', async () => {
    const encoder = new Encoder().writeBuffer(Buffer.alloc(64, 'a'));
    const compressed = await lz4Codec.compress(encoder);
    await expect(decompressLz4(compressed, 1)).rejects.toBeInstanceOf(KafkaNonRetriableError);
  });

  it('propagates codec errors that are not a size-cap violation', async () => {
    await expect(decompressLz4(Buffer.from('not lz4'))).rejects.toThrow();
    await expect(decompressLz4(Buffer.from('not lz4'))).rejects.not.toBeInstanceOf(KafkaNonRetriableError);
  });

  it('does not resolve compress on the calling tick (off-thread or native async)', () => {
    const encoder = new Encoder().writeBuffer(Buffer.alloc(8 * 1024, 'a'));
    const pending = lz4Codec.compress(encoder);
    expect(inspect(pending)).toContain('pending');
    return pending;
  });

  it('round-trips concurrent compress/decompress without mixing payloads', async () => {
    const payloads = ['alpha', 'bravo', 'charlie', 'delta'].map((value) => new Encoder().writeString(value));
    const compressed = await Promise.all(payloads.map((encoder) => lz4Codec.compress(encoder)));
    const decompressed = await Promise.all(compressed.map((buf) => lz4Codec.decompress(buf)));
    expect(decompressed.map((buf) => buf.toString())).toEqual(payloads.map((encoder) => encoder.buffer.toString()));
  });
});
