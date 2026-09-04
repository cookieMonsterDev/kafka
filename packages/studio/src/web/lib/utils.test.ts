import { describe, expect, it } from 'vitest';
import { formatBytes } from './utils';

describe('formatBytes', () => {
  it('formats null as an em dash', () => {
    expect(formatBytes(null)).toBe('—');
  });

  it('formats zero bytes', () => {
    expect(formatBytes('0')).toBe('0 B');
  });

  it('formats sub-kilobyte sizes as whole bytes', () => {
    expect(formatBytes('512')).toBe('512 B');
  });

  it('rolls over to the next unit at 1024', () => {
    expect(formatBytes('1024')).toBe('1.0 KB');
    expect(formatBytes(String(5 * 1024 * 1024))).toBe('5.0 MB');
  });

  it('treats a negative value as unavailable', () => {
    expect(formatBytes('-1')).toBe('—');
  });

  it('treats an unparseable value as unavailable', () => {
    expect(formatBytes('not-a-number')).toBe('—');
  });
});
