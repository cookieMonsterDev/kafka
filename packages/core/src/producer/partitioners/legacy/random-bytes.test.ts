import { describe, expect, it } from 'vitest';
import { KafkaNonRetriableError } from '../../../errors';
import { randomBytes } from './random-bytes';

describe('producer/partitioners/legacy/randomBytes', () => {
  it('throws when requesting more bytes than the entropy budget allows', () => {
    expect(() => randomBytes(65537)).toThrow(
      new KafkaNonRetriableError('Byte length (65537) exceeds the max number of bytes of entropy available (65536)'),
    );
  });

  it('returns random bytes of the desired length', () => {
    const bytes = randomBytes(32);
    expect(Buffer.isBuffer(bytes)).toBe(true);
    expect(bytes.byteLength).toBe(32);
  });

  // Web Crypto's `getRandomValues` rejects more than 65536 bytes, so the limit must not exceed it.
  it('returns the maximum entropy budget in one call', () => {
    expect(randomBytes(65536).byteLength).toBe(65536);
  });
});
