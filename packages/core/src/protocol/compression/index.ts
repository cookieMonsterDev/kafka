import { KafkaJSNotImplemented } from '../../errors.js';
import type { Encoder } from '../encoder.js';
import { gzipCodec } from './gzip.js';
import { zstdCodec } from './zstd.js';

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

function notImplemented(name: string): CompressionCodecFactory {
  return () => {
    throw new KafkaJSNotImplemented(`${name} compression not implemented`);
  };
}

/**
 * GZIP and ZSTD are built in; Snappy and LZ4 stay pluggable so a user can install a codec
 * package and register it: `CompressionCodecs[CompressionTypes.Snappy] = () => mySnappyCodec`.
 */
export const CompressionCodecs: Record<number, CompressionCodecFactory> = {
  [COMPRESSION_TYPES.GZIP]: () => gzipCodec,
  [COMPRESSION_TYPES.ZSTD]: () => zstdCodec,
  [COMPRESSION_TYPES.Snappy]: notImplemented('Snappy'),
  [COMPRESSION_TYPES.LZ4]: notImplemented('LZ4'),
};

export function lookupCodec(type: number): CompressionCodec | null {
  const factory = CompressionCodecs[type];
  return factory ? factory() : null;
}

export function lookupCodecByAttributes(attributes: number): CompressionCodec | null {
  return lookupCodec(attributes & COMPRESSION_CODEC_MASK);
}
