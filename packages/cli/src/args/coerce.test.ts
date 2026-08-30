import { describe, expect, it } from 'vitest';
import { CliUsageError, coerceEnum, coerceKeyValueRecord, coerceNumber, splitKeyValue } from './coerce';

describe('coerceNumber', () => {
  it('parses an integer', () => {
    expect(coerceNumber('42', 'partitions')).toBe(42);
  });

  it('parses a negative and a decimal', () => {
    expect(coerceNumber('-3', 'skew')).toBe(-3);
    expect(coerceNumber('1.5', 'factor')).toBe(1.5);
  });

  it('throws CliUsageError naming the flag on non-numeric input', () => {
    expect(() => coerceNumber('abc', 'partitions')).toThrow(CliUsageError);
    expect(() => coerceNumber('abc', 'partitions')).toThrow(/--partitions/);
  });

  it('throws on a blank string rather than coercing to 0', () => {
    expect(() => coerceNumber('', 'timeout')).toThrow(CliUsageError);
    expect(() => coerceNumber('   ', 'timeout')).toThrow(CliUsageError);
  });
});

describe('coerceEnum', () => {
  const values = ['earliest', 'latest', 'max-timestamp'] as const;

  it('accepts a value in the closed set', () => {
    expect(coerceEnum('earliest', 'time', values)).toBe('earliest');
  });

  it('throws listing the valid values on an unknown value', () => {
    expect(() => coerceEnum('yesterday', 'time', values)).toThrow(CliUsageError);
    expect(() => coerceEnum('yesterday', 'time', values)).toThrow(/earliest, latest, max-timestamp/);
  });
});

describe('splitKeyValue', () => {
  it('splits key=value on the first "="', () => {
    expect(splitKeyValue('retention.ms=60000', 'config')).toEqual(['retention.ms', '60000']);
  });

  it('keeps everything after the first "=" as the value', () => {
    expect(splitKeyValue('a=b=c', 'config')).toEqual(['a', 'b=c']);
  });

  it('throws on a missing "="', () => {
    expect(() => splitKeyValue('retention.ms', 'config')).toThrow(CliUsageError);
  });

  it('throws on an empty key', () => {
    expect(() => splitKeyValue('=60000', 'config')).toThrow(CliUsageError);
  });
});

describe('coerceKeyValueRecord', () => {
  it('decodes repeated entries into a record', () => {
    expect(coerceKeyValueRecord(['a=1', 'b=2'], 'config')).toEqual({ a: '1', b: '2' });
  });

  it('returns an empty record for no entries', () => {
    expect(coerceKeyValueRecord([], 'config')).toEqual({});
  });

  it('later duplicate keys win', () => {
    expect(coerceKeyValueRecord(['a=1', 'a=2'], 'config')).toEqual({ a: '2' });
  });
});
