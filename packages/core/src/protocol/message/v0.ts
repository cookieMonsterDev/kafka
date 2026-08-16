import { COMPRESSION_CODEC_MASK, COMPRESSION_TYPES, type CompressionType } from '../compression/index';
import { crc32 } from '../crc32';
import type { Decoder } from '../decoder';
import { Encoder } from '../encoder';

/**
 * v0
 * Message => Crc MagicByte Attributes Key Value
 *   Crc => int32
 *   MagicByte => int8
 *   Attributes => int8
 *   Key => bytes
 *   Value => bytes
 */
export function encodeMessageV0({
  compression = COMPRESSION_TYPES.None,
  key,
  value,
}: {
  compression?: CompressionType;
  key?: Buffer | string | null;
  value?: Buffer | string | null;
}): Encoder {
  const content = new Encoder()
    .writeInt8(0)
    .writeInt8(compression & COMPRESSION_CODEC_MASK)
    .writeBytes(key)
    .writeBytes(value);

  return new Encoder().writeInt32(crc32(content)).writeEncoder(content);
}

export function decodeMessageV0(decoder: Decoder): {
  attributes: number;
  key: Buffer | null;
  value: Buffer | null;
} {
  return {
    attributes: decoder.readInt8(),
    key: decoder.readBytes(),
    value: decoder.readBytes(),
  };
}
