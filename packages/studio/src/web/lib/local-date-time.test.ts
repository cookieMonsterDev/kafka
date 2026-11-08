import { describe, expect, it } from 'vitest';
import { formatLocalDateTime, parseLocalDateTime } from './local-date-time';

describe('local date-time picker values', () => {
  it('round-trips local calendar fields without converting them to UTC', () => {
    const date = new Date(2026, 10, 7, 23, 45);
    expect(formatLocalDateTime(date)).toBe('2026-11-07T23:45');
    const parsed = parseLocalDateTime('2026-11-07T23:45');
    expect(parsed?.getFullYear()).toBe(2026);
    expect(parsed?.getMonth()).toBe(10);
    expect(parsed?.getDate()).toBe(7);
    expect(parsed?.getHours()).toBe(23);
    expect(parsed?.getMinutes()).toBe(45);
  });
  it('rejects empty, malformed, invalid calendar days, and out-of-range clock fields', () => {
    for (const value of ['', 'tomorrow', '2026-02-30T12:00', '2026-01-01T25:00', '2026-01-01T12:61']) {
      expect(parseLocalDateTime(value)).toBeUndefined();
    }
  });
  it('supports leap days and midnight', () => {
    expect(formatLocalDateTime(parseLocalDateTime('2028-02-29T00:00') ?? new Date(NaN))).toBe('2028-02-29T00:00');
  });
});
