import { describe, expect, it } from 'vitest';
import { loadOptionalNativeLz4, loadOptionalNativeSnappy } from './optional-native';

describe('protocol/compression/optional-native', () => {
  it('does not throw when optional native packages are not installed', async () => {
    expect(() => loadOptionalNativeSnappy()).not.toThrow();
    await expect(loadOptionalNativeLz4()).resolves.not.toBeUndefined();
  });

  it('returns null or an async codec, never a sync implementation', async () => {
    const snappy = loadOptionalNativeSnappy();
    expect(snappy === null || typeof snappy.compress === 'function').toBe(true);
    const snappyProbe = snappy ? await snappy.compress(Buffer.from('native-snappy-probe')) : null;
    expect(snappyProbe === null || Buffer.isBuffer(snappyProbe)).toBe(true);

    const lz4 = await loadOptionalNativeLz4();
    expect(lz4 === null || typeof lz4.compress === 'function').toBe(true);
    const lz4Probe = lz4 ? await lz4.compress(Buffer.from('native-lz4-probe')) : null;
    expect(lz4Probe === null || lz4Probe.subarray(0, 4).equals(Buffer.from([0x04, 0x22, 0x4d, 0x18]))).toBe(true);
  });
});
