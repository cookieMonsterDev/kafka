import { COMPRESSION_CODEC_MASK, COMPRESSION_TYPES, type CompressionType } from '../compression/index';
import { crc32 } from '../crc32';
import type { Decoder } from '../decoder';
import { Encoder } from '../encoder';

/**
 * v1 (supported since 0.10.0)
 * Message => Crc MagicByte Attributes Timestamp Key Value
 *   Crc => int32
 *   MagicByte => int8
 *   Attributes => int8
 *   Timestamp => int64
 *   Key => bytes
 *   Value => bytes
 */
export function encodeMessageV1({
  compression = COMPRESSION_TYPES.None,
  timestamp = Date.now(),
  key,
  value,
}: {
  compression?: CompressionType;
  timestamp?: number;
  key?: Buffer | string | null;
  value?: Buffer | string | null;
}): Encoder {
  const content = new Encoder()
    .writeInt8(1)
    .writeInt8(compression & COMPRESSION_CODEC_MASK)
    .writeInt64(timestamp)
    .writeBytes(key)
    .writeBytes(value);

  return new Encoder().writeInt32(crc32(content)).writeEncoder(content, { release: true });
}

export function decodeMessageV1(decoder: Decoder): {
  attributes: number;
  timestamp: bigint;
  key: Buffer | null;
  value: Buffer | null;
} {
  return {
    attributes: decoder.readInt8(),
    timestamp: decoder.readInt64(),
    key: decoder.readBytes(),
    value: decoder.readBytes(),
  };
}
