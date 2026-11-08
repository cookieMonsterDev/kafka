import { describe, expect, it } from 'vitest';
import { formatConfigDuration } from './topic-config-display';

describe('formatConfigDuration', () => {
  it('shows durations without changing the raw config value', () => {
    expect(formatConfigDuration('retention.ms', '86400000')).toBe('1 d');
    expect(formatConfigDuration('file.delete.delay.ms', '60000')).toBe('1 m');
    expect(formatConfigDuration('retention.ms', '90061001')).toBe('1 d 1 h 1 m 1 s 1 ms');
    expect(formatConfigDuration('retention.ms', '0')).toBe('0 ms');
  });

  it('does not reinterpret negative sentinels, unknown units, or invalid drafts', () => {
    for (const value of ['-1', '-2', '', '1.5', 'abc']) {
      expect(formatConfigDuration('retention.ms', value)).toBeNull();
    }
    expect(formatConfigDuration('retention.bytes', '60000')).toBeNull();
  });

  it('preserves precision beyond the safe integer range', () => {
    expect(formatConfigDuration('retention.ms', '9007199254740993')).toBe('104249991 d 8 h 59 m 993 ms');
  });
});
