import type { Encoder } from '../encoder';
import { gzipCodec } from './gzip';
import { lz4Codec } from './lz4';
import { snappyCodec } from './snappy';
import { zstdCodec } from './zstd';

/**
 * Record-batch compression codecs. GZIP, Snappy, LZ4, and ZSTD are built in.
 * @see https://kafka.apache.org/43/implementation/messages/
 */
export const COMPRESSION_TYPES = Object.freeze({
  None: 0,
  GZIP: 1,
  Snappy: 2,
  LZ4: 3,
  ZSTD: 4,
});

export type CompressionType = (typeof COMPRESSION_TYPES)[keyof typeof COMPRESSION_TYPES];

export const COMPRESSION_CODEC_MASK = 0x07;

export interface CompressionCodec {
  compress(encoder: Encoder): Promise<Buffer>;
  decompress(buffer: Buffer): Promise<Buffer>;
}

export type CompressionCodecFactory = () => CompressionCodec;

/**
 * GZIP, Snappy, LZ4, and ZSTD are built in. Built-in entries remain overridable
 * through this mutable registry:
 * `CompressionCodecs[CompressionTypes.LZ4] = () => myLz4Codec`.
 */
export const CompressionCodecs: Record<number, CompressionCodecFactory> = {
  [COMPRESSION_TYPES.GZIP]: () => gzipCodec,
  [COMPRESSION_TYPES.Snappy]: () => snappyCodec,
  [COMPRESSION_TYPES.LZ4]: () => lz4Codec,
  [COMPRESSION_TYPES.ZSTD]: () => zstdCodec,
};

export function lookupCodec(type: number): CompressionCodec | null {
  const factory = CompressionCodecs[type];
  return factory ? factory() : null;
}

export function lookupCodecByAttributes(attributes: number): CompressionCodec | null {
  return lookupCodec(attributes & COMPRESSION_CODEC_MASK);
}
