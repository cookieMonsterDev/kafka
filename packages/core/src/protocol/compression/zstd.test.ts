import { promisify } from 'node:util';
import { zstdDecompress } from 'node:zlib';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { KafkaNonRetriableError } from '../../errors';
import { isBun } from '../../runtime/runtime';
import { Encoder } from '../encoder';
import { decompressZstd, zstdCodec } from './zstd';

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

  it('rejects decompressed output that exceeds the size cap', async () => {
    const encoder = new Encoder().writeBuffer(Buffer.alloc(64, 'a'));
    const compressed = await zstdCodec.compress(encoder);
    await expect(decompressZstd(compressed, 1)).rejects.toBeInstanceOf(KafkaNonRetriableError);
  });

  it('propagates zlib errors that are not a size-cap violation', async () => {
    await expect(decompressZstd(Buffer.from('not zstd'))).rejects.toThrow();
    await expect(decompressZstd(Buffer.from('not zstd'))).rejects.not.toBeInstanceOf(KafkaNonRetriableError);
  });

  it('honors compressionLevel: level 1 produces a larger output than level 19 for compressible data', async () => {
    // A single repeated token compresses to the same minimal size at every level, so use data
    // with enough distinct-but-recurring structure that higher search effort (a higher level)
    // measurably finds better matches.
    const payload = Array.from({ length: 8000 }, (_, i) => `record-${i % 37}-value-${(i * 7) % 53}`).join(',');
    const encoder = new Encoder().writeBuffer(Buffer.from(payload));
    const fast = await zstdCodec.compress(encoder, 1);
    const best = await zstdCodec.compress(encoder, 19);

    expect(fast.length).toBeGreaterThan(best.length);
    expect(await zstdCodec.decompress(fast)).toEqual(encoder.buffer);
    expect(await zstdCodec.decompress(best)).toEqual(encoder.buffer);
  });

  describe('compression path per runtime', () => {
    const nodeDecompress = promisify(zstdDecompress);
    const multiBlock = Buffer.from(
      Array.from({ length: 20_000 }, (_, i) => `record-${i}-${(i * 2654435761) % 100_003};`).join(''),
    );

    afterEach(() => {
      vi.restoreAllMocks();
      vi.unstubAllEnvs();
    });

    it.each([undefined, 1, 19])(
      'round-trips empty, tiny and multi-block input through node:zlib (level %s)',
      async (level) => {
        for (const input of [Buffer.alloc(0), Buffer.from('k'), multiBlock]) {
          const compressed = await zstdCodec.compress(new Encoder().writeBuffer(input), level);
          const expected = new Encoder().writeBuffer(input).buffer;

          expect(Buffer.isBuffer(compressed)).toBe(true);
          expect((await nodeDecompress(compressed)).equals(expected)).toBe(true);
        }
      },
    );

    it.each([-5, 0, 23, 1.5])(
      'keeps level %s working although Bun.zstdCompress only takes integers 1 to 22',
      async (level) => {
        const encoder = new Encoder().writeString('levels outside 1..22');
        const compressed = await zstdCodec.compress(encoder, level);
        expect(await zstdCodec.decompress(compressed)).toEqual(encoder.buffer);
      },
    );

    it.runIf(isBun())('uses Bun.zstdCompress on Bun unless KAFKA_BUN_NATIVE=0', async () => {
      const bun = (globalThis as unknown as { Bun: { zstdCompress: (input: Uint8Array) => Promise<Buffer> } }).Bun;
      const spy = vi.spyOn(bun, 'zstdCompress');
      const encoder = new Encoder().writeString('bun native');

      vi.stubEnv('KAFKA_BUN_NATIVE', '1');
      await zstdCodec.compress(encoder, 3);
      expect(spy).toHaveBeenCalledTimes(1);

      vi.stubEnv('KAFKA_BUN_NATIVE', '0');
      await zstdCodec.compress(encoder, 3);
      expect(spy).toHaveBeenCalledTimes(1);
    });
  });
});
