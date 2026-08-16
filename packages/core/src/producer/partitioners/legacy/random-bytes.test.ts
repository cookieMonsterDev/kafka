import { describe, expect, it } from 'vitest';
import { KafkaJSNonRetriableError } from '../../../errors.js';
import { randomBytes } from './random-bytes.js';

describe('producer/partitioners/legacy/randomBytes', () => {
  it('throws when requesting more bytes than the entropy budget allows', () => {
    expect(() => randomBytes(65537)).toThrow(
      new KafkaJSNonRetriableError('Byte length (65537) exceeds the max number of bytes of entropy available (65536)'),
    );
  });

  it('returns random bytes of the desired length', () => {
    const bytes = randomBytes(32);
    expect(Buffer.isBuffer(bytes)).toBe(true);
    expect(bytes.byteLength).toBe(32);
  });
});
