import { randomBytes as nodeRandomBytes } from 'node:crypto';
import { KafkaNonRetriableError } from '../../../errors';

const MAX_BYTES = 65536;

/** Cryptographically strong random bytes via `node:crypto`. */
export function randomBytes(size: number): Buffer {
  if (size > MAX_BYTES) {
    throw new KafkaNonRetriableError(
      `Byte length (${size}) exceeds the max number of bytes of entropy available (${MAX_BYTES})`,
    );
  }

  return nodeRandomBytes(size);
}
