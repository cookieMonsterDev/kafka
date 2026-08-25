import { KafkaCorruptRecordError, KafkaPartialMessageError, KafkaUnsupportedMagicByteInMessageSet } from '../../errors';
import { crc32 } from '../crc32';
import type { Decoder } from '../decoder';
import { decodeMessageV0 } from './v0';
import { decodeMessageV1 } from './v1';

export interface DecodedMessageBody {
  attributes: number;
  timestamp?: bigint;
  key: Buffer | null;
  value: Buffer | null;
}

function decodeMessageBody(decoder: Decoder, magicByte: number): DecodedMessageBody {
  switch (magicByte) {
    case 0:
      return decodeMessageV0(decoder);
    case 1:
      return decodeMessageV1(decoder);
    default:
      throw new KafkaUnsupportedMagicByteInMessageSet(
        `Unsupported MessageSet message version, magic byte: ${magicByte}`,
      );
  }
}

export interface DecodedMessage extends DecodedMessageBody {
  offset: bigint;
  size: number;
  crc: number;
  magicByte: number;
}

export function decodeMessage(offset: bigint, size: number, decoder: Decoder, checkCrcs = true): DecodedMessage {
  const remainingBytes = Buffer.byteLength(decoder.slice(size).buffer);
  if (remainingBytes < size) {
    throw new KafkaPartialMessageError(
      `Tried to decode a partial message: remainingBytes(${remainingBytes}) < messageSize(${size})`,
    );
  }

  const crc = decoder.readInt32();
  // The CRC covers everything after the CRC field itself (magicByte, attributes, [timestamp],
  // key, value) - exactly the `size - 4` bytes starting here, regardless of how far the body
  // decode below advances the decoder's offset.
  const contentStart = decoder.offset;
  const magicByte = decoder.readInt8();
  const message = decodeMessageBody(decoder, magicByte);

  if (checkCrcs) {
    const content = decoder.buffer.subarray(contentStart, contentStart + (size - 4));
    const computedCrc = crc32(content);
    if (computedCrc !== crc) {
      throw new KafkaCorruptRecordError(`MessageSet CRC mismatch: expected ${crc}, computed ${computedCrc}`, {
        expectedCrc: crc,
        computedCrc,
      });
    }
  }

  return { offset, size, crc, magicByte, ...message };
}
