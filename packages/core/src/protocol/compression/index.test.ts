import { describe, expect, it } from 'vitest';
import type { Encoder } from '../encoder';
import {
  COMPRESSION_CODEC_MASK,
  COMPRESSION_TYPES,
  CompressionCodecs,
  lookupCodec,
  lookupCodecByAttributes,
} from './index';

describe('protocol/compression', () => {
  it('returns null for CompressionTypes.None', () => {
    expect(lookupCodec(COMPRESSION_TYPES.None)).toBeNull();
  });

  it('resolves the GZIP, Snappy, LZ4, and ZSTD codecs', () => {
    expect(lookupCodec(COMPRESSION_TYPES.GZIP)).not.toBeNull();
    expect(lookupCodec(COMPRESSION_TYPES.Snappy)).not.toBeNull();
    expect(lookupCodec(COMPRESSION_TYPES.LZ4)).not.toBeNull();
    expect(lookupCodec(COMPRESSION_TYPES.ZSTD)).not.toBeNull();
  });

  it('lets a user override a built-in codec', () => {
    const original = CompressionCodecs[COMPRESSION_TYPES.LZ4];
    const fakeCodec = { compress: async (e: Encoder) => e.buffer, decompress: async (b: Buffer) => b };
    CompressionCodecs[COMPRESSION_TYPES.LZ4] = () => fakeCodec;
    try {
      expect(lookupCodec(COMPRESSION_TYPES.LZ4)).toBe(fakeCodec);
    } finally {
      if (original) CompressionCodecs[COMPRESSION_TYPES.LZ4] = original;
    }
  });

  it('lookupCodecByAttributes masks the low 3 bits to find the codec', () => {
    const attributesWithGzipAndOtherBitsSet = COMPRESSION_TYPES.GZIP | 0b1000;
    expect(lookupCodecByAttributes(attributesWithGzipAndOtherBitsSet)).not.toBeNull();
    expect(COMPRESSION_CODEC_MASK).toBe(0x07);
  });
});
