import { randomBytes as nodeRandomBytes } from 'node:crypto';
import { KafkaJSNonRetriableError } from '../../../errors.js';

const MAX_BYTES = 65536;

/**
 * kafkajs's own version of this juggles a `global.crypto`/`global.msCrypto`/`require('crypto')`
 * fallback chain to run in browsers as well as Node. This port targets Node only, so `node:crypto`
 * is unconditionally available and that whole detection dance is dead weight.
 */
export function randomBytes(size: number): Buffer {
  if (size > MAX_BYTES) {
    throw new KafkaJSNonRetriableError(
      `Byte length (${size}) exceeds the max number of bytes of entropy available (${MAX_BYTES})`,
    );
  }

  return nodeRandomBytes(size);
}
