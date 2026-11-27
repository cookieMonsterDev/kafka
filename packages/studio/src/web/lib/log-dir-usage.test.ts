import { describe, expect, it } from 'vitest';
import { logDirUsedPercent } from './log-dir-usage';

describe('logDirUsedPercent', () => {
  it('rounds the used share of the volume to a whole percentage', () => {
    expect(logDirUsedPercent({ totalBytes: '1000', usedBytes: '375' })).toBe(38);
    expect(logDirUsedPercent({ totalBytes: '1000', usedBytes: '0' })).toBe(0);
    expect(logDirUsedPercent({ totalBytes: '1000', usedBytes: '1000' })).toBe(100);
  });

  it('returns null when the broker reported no sizes', () => {
    expect(logDirUsedPercent({ totalBytes: null, usedBytes: null })).toBeNull();
  });

  it('returns null for an empty volume instead of dividing by zero', () => {
    expect(logDirUsedPercent({ totalBytes: '0', usedBytes: '0' })).toBeNull();
  });
});
