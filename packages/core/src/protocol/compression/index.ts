import { KafkaNotImplemented } from '../../errors';
import type { Encoder } from '../encoder';
import { gzipCodec } from './gzip';
import { lz4Codec } from './lz4';
import { zstdCodec } from './zstd';

/**
 * Record-batch compression codecs. GZIP, LZ4, and ZSTD are built in; Snappy is pluggable.
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

function notImplemented(name: string): CompressionCodecFactory {
  return () => {
    throw new KafkaNotImplemented(`${name} compression not implemented`);
  };
}

/**
 * GZIP, LZ4, and ZSTD are built in; Snappy stays pluggable so a user can install a codec
 * package and register it: `CompressionCodecs[CompressionTypes.Snappy] = () => mySnappyCodec`.
 */
export const CompressionCodecs: Record<number, CompressionCodecFactory> = {
  [COMPRESSION_TYPES.GZIP]: () => gzipCodec,
  [COMPRESSION_TYPES.LZ4]: () => lz4Codec,
  [COMPRESSION_TYPES.ZSTD]: () => zstdCodec,
  [COMPRESSION_TYPES.Snappy]: notImplemented('Snappy'),
};

export function lookupCodec(type: number): CompressionCodec | null {
  const factory = CompressionCodecs[type];
  return factory ? factory() : null;
}

export function lookupCodecByAttributes(attributes: number): CompressionCodec | null {
  return lookupCodec(attributes & COMPRESSION_CODEC_MASK);
}
