import { describe, expect, it } from 'vitest';
import type { Encoder } from '../encoder.js';
import {
  COMPRESSION_CODEC_MASK,
  COMPRESSION_TYPES,
  CompressionCodecs,
  lookupCodec,
  lookupCodecByAttributes,
} from './index.js';

describe('protocol/compression', () => {
  it('returns null for CompressionTypes.None', () => {
    expect(lookupCodec(COMPRESSION_TYPES.None)).toBeNull();
  });

  it('resolves the GZIP and ZSTD codecs', () => {
    expect(lookupCodec(COMPRESSION_TYPES.GZIP)).not.toBeNull();
    expect(lookupCodec(COMPRESSION_TYPES.ZSTD)).not.toBeNull();
  });

  it('throws KafkaJSNotImplemented for Snappy and LZ4 out of the box', () => {
    expect(() => lookupCodec(COMPRESSION_TYPES.Snappy)).toThrow('Snappy compression not implemented');
    expect(() => lookupCodec(COMPRESSION_TYPES.LZ4)).toThrow('LZ4 compression not implemented');
  });

  it('lets a user register a codec for Snappy/LZ4', () => {
    const original = CompressionCodecs[COMPRESSION_TYPES.Snappy];
    const fakeCodec = { compress: async (e: Encoder) => e.buffer, decompress: async (b: Buffer) => b };
    CompressionCodecs[COMPRESSION_TYPES.Snappy] = () => fakeCodec;
    try {
      expect(lookupCodec(COMPRESSION_TYPES.Snappy)).toBe(fakeCodec);
    } finally {
      if (original) CompressionCodecs[COMPRESSION_TYPES.Snappy] = original;
    }
  });

  it('lookupCodecByAttributes masks the low 3 bits to find the codec', () => {
    const attributesWithGzipAndOtherBitsSet = COMPRESSION_TYPES.GZIP | 0b1000;
    expect(lookupCodecByAttributes(attributesWithGzipAndOtherBitsSet)).not.toBeNull();
    expect(COMPRESSION_CODEC_MASK).toBe(0x07);
  });
});
