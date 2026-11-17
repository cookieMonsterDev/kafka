import { KafkaNonRetriableError } from '../../../errors';

const MAX_BYTES = 65536;

/**
 * Cryptographically strong random bytes via Web Crypto, which Node.js and Bun expose globally, so
 * importing the client does not load `node:crypto`.
 */
export function randomBytes(size: number): Buffer {
  if (size > MAX_BYTES) {
    throw new KafkaNonRetriableError(
      `Byte length (${size}) exceeds the max number of bytes of entropy available (${MAX_BYTES})`,
    );
  }

  return globalThis.crypto.getRandomValues(Buffer.allocUnsafe(size));
}
