import type { CompressionType } from '../compression/index';
import type { Encoder } from '../encoder';
import { encodeMessageV0 } from './v0';
import { encodeMessageV1 } from './v1';

export interface EncodeMessageOptions {
  compression?: CompressionType;
  timestamp?: number;
  key?: Buffer | string | null;
  value?: Buffer | string | null;
}

export type MessageVersion = 0 | 1;

export function encodeMessage(version: MessageVersion, entry: EncodeMessageOptions): Encoder {
  return version === 1 ? encodeMessageV1(entry) : encodeMessageV0(entry);
}

export { decodeMessage, type DecodedMessage } from './decoder';
