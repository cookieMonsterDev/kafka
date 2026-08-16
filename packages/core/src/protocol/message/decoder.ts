import { KafkaPartialMessageError, KafkaUnsupportedMagicByteInMessageSet } from '../../errors';
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

export function decodeMessage(offset: bigint, size: number, decoder: Decoder): DecodedMessage {
  const remainingBytes = Buffer.byteLength(decoder.slice(size).buffer);
  if (remainingBytes < size) {
    throw new KafkaPartialMessageError(
      `Tried to decode a partial message: remainingBytes(${remainingBytes}) < messageSize(${size})`,
    );
  }

  const crc = decoder.readInt32();
  const magicByte = decoder.readInt8();
  const message = decodeMessageBody(decoder, magicByte);
  return { offset, size, crc, magicByte, ...message };
}
