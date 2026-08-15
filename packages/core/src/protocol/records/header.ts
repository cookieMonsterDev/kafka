import type { Decoder } from '../decoder.js';
import { Encoder } from '../encoder.js';

/**
 * v2
 * Header => Key Value
 *   Key => varInt|string
 *   Value => varInt|bytes
 */

export interface RecordHeaderInput {
  key: string;
  value: Buffer | string | null;
}

export interface DecodedRecordHeader {
  key: string | null;
  value: Buffer | null;
}

export function encodeHeader({ key, value }: RecordHeaderInput): Encoder {
  return new Encoder().writeVarIntString(key).writeVarIntBytes(value);
}

export function decodeHeader(decoder: Decoder): DecodedRecordHeader {
  return {
    key: decoder.readVarIntString(),
    value: decoder.readVarIntBytes(),
  };
}
