import { compress as snappyCompress, uncompress as snappyUncompress } from 'snappyjs';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { KafkaNonRetriableError } from '../../errors';
import { Encoder } from '../encoder';

const resolveOptionalNativeSnappy = vi.fn();

vi.mock('./optional-native', () => ({
  resolveOptionalNativeSnappy: () => resolveOptionalNativeSnappy(),
}));

const XERIAL_MAGIC = Buffer.from([0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00]);

function fakeNativeCodec() {
  return {
    compress: async (buffer: Buffer) => Buffer.from(snappyCompress(buffer)),
    decompress: async (buffer: Buffer) => Buffer.from(snappyUncompress(buffer)),
  };
}

describe('protocol/compression/snappy (native codec path)', () => {
  beforeEach(() => {
    resolveOptionalNativeSnappy.mockReset();
    resolveOptionalNativeSnappy.mockReturnValue(fakeNativeCodec());
  });

  it('round-trips through the native codec, still producing xerial framing', async () => {
    const { snappyCodec } = await import('./snappy');
    const encoder = new Encoder().writeString('native snappy path');
    const compressed = await snappyCodec.compress(encoder);
    expect(compressed.subarray(0, 8)).toEqual(XERIAL_MAGIC);
    expect(await snappyCodec.decompress(compressed)).toEqual(encoder.buffer);
  });

  it('round-trips payloads spanning multiple xerial blocks via native codec', async () => {
    const { snappyCodec } = await import('./snappy');
    const encoder = new Encoder().writeBuffer(Buffer.alloc(40 * 1024, 'n'));
    const compressed = await snappyCodec.compress(encoder);
    expect(await snappyCodec.decompress(compressed)).toEqual(encoder.buffer);
  });

  it('decompresses a raw (unframed) block via the native codec', async () => {
    const { decompressSnappy } = await import('./snappy');
    const raw = Buffer.from('raw native block');
    const compressed = Buffer.from(snappyCompress(raw));
    expect(await decompressSnappy(compressed)).toEqual(raw);
  });

  it('rejects raw native output exceeding the size cap', async () => {
    const { decompressSnappy } = await import('./snappy');
    const raw = Buffer.alloc(128, 'x');
    const compressed = Buffer.from(snappyCompress(raw));
    await expect(decompressSnappy(compressed, 1)).rejects.toBeInstanceOf(KafkaNonRetriableError);
  });

  it('rejects xerial-framed native output exceeding the size cap', async () => {
    const { snappyCodec, decompressSnappy } = await import('./snappy');
    const encoder = new Encoder().writeBuffer(Buffer.alloc(128, 'y'));
    const compressed = await snappyCodec.compress(encoder);
    await expect(decompressSnappy(compressed, 1)).rejects.toBeInstanceOf(KafkaNonRetriableError);
  });

  it('throws when native becomes unavailable between the outer check and the call', async () => {
    resolveOptionalNativeSnappy.mockReset();
    resolveOptionalNativeSnappy.mockReturnValueOnce(fakeNativeCodec()).mockReturnValueOnce(null);
    const { decompressSnappy } = await import('./snappy');
    await expect(decompressSnappy(Buffer.from('anything'))).rejects.toThrow('native snappy is not available');
  });
});
