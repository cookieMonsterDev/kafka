import { afterEach, describe, expect, it, vi } from 'vitest';
import { isBun, isBunNativeEnabled } from './runtime';

describe('isBunNativeEnabled', () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it.skipIf(isBun())('is false on Node.js regardless of KAFKA_BUN_NATIVE', () => {
    vi.stubEnv('KAFKA_BUN_NATIVE', '1');
    expect(isBunNativeEnabled()).toBe(false);
  });

  it.runIf(isBun())('follows KAFKA_BUN_NATIVE on Bun', () => {
    vi.stubEnv('KAFKA_BUN_NATIVE', '0');
    expect(isBunNativeEnabled()).toBe(false);
    vi.stubEnv('KAFKA_BUN_NATIVE', '');
    expect(isBunNativeEnabled()).toBe(true);
  });
});
