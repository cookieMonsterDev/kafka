import { KafkaNotImplemented } from '../../errors';
import type { Encoder } from '../encoder';
import { gzipCodec } from './gzip';
import { snappyCodec } from './snappy';
import { zstdCodec } from './zstd';

/**
 * Record-batch compression codecs. GZIP, Snappy, and ZSTD are built in; LZ4 is pluggable.
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
 * GZIP, Snappy, and ZSTD are built in. LZ4 stays a pluggable stub so a user can install a
 * codec package and register it: `CompressionCodecs[CompressionTypes.LZ4] = () => myLz4Codec`.
 * Built-in entries remain overridable through this mutable registry.
 */
export const CompressionCodecs: Record<number, CompressionCodecFactory> = {
  [COMPRESSION_TYPES.GZIP]: () => gzipCodec,
  [COMPRESSION_TYPES.Snappy]: () => snappyCodec,
  [COMPRESSION_TYPES.ZSTD]: () => zstdCodec,
  [COMPRESSION_TYPES.LZ4]: notImplemented('LZ4'),
};

export function lookupCodec(type: number): CompressionCodec | null {
  const factory = CompressionCodecs[type];
  return factory ? factory() : null;
}

export function lookupCodecByAttributes(attributes: number): CompressionCodec | null {
  return lookupCodec(attributes & COMPRESSION_CODEC_MASK);
}
