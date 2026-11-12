import { Encoder } from '../encoder';

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

export function encodeHeader({ key, value }: RecordHeaderInput): Encoder {
  return new Encoder().writeVarIntString(key).writeVarIntBytes(value);
}
