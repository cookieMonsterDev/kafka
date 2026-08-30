import { describe, expect, it } from 'vitest';
import { stringifyJsonSafe, toJsonSafe } from './json';

describe('toJsonSafe', () => {
  it('converts a bigint past 2^53 to a string without losing precision', () => {
    const value = 2n ** 53n + 1n;
    expect(toJsonSafe(value)).toBe(value.toString());
    expect(toJsonSafe(value)).toBe('9007199254740993');
  });

  it('converts a bigint at 2^63-1 to a string without losing precision', () => {
    const value = 2n ** 63n - 1n;
    expect(toJsonSafe(value)).toBe('9223372036854775807');
  });

  it('converts a Buffer to base64', () => {
    const buffer = Buffer.from('hello');
    expect(toJsonSafe(buffer)).toBe(buffer.toString('base64'));
  });

  it('converts a 16-byte Buffer under a topicId key to a UUID string', () => {
    const buffer = Buffer.from('0123456789abcdef', 'utf8');
    expect(buffer.length).toBe(16);
    const result = toJsonSafe({ topicId: buffer }) as { topicId: string };
    expect(result.topicId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
    expect(result.topicId.replaceAll('-', '')).toBe(buffer.toString('hex'));
  });

  it('does not UUID-format a 16-byte Buffer under any other key', () => {
    const buffer = Buffer.from('0123456789abcdef', 'utf8');
    const result = toJsonSafe({ other: buffer }) as { other: string };
    expect(result.other).toBe(buffer.toString('base64'));
  });

  it('does not UUID-format a topicId Buffer of the wrong length', () => {
    const buffer = Buffer.from('short');
    const result = toJsonSafe({ topicId: buffer }) as { topicId: string };
    expect(result.topicId).toBe(buffer.toString('base64'));
  });

  it('recurses through arrays and nested objects', () => {
    const value = { partitions: [{ offset: 5n }, { offset: 10n }] };
    expect(toJsonSafe(value)).toEqual({ partitions: [{ offset: '5' }, { offset: '10' }] });
  });

  it('passes through primitives untouched', () => {
    expect(toJsonSafe('a string')).toBe('a string');
    expect(toJsonSafe(42)).toBe(42);
    expect(toJsonSafe(true)).toBe(true);
    expect(toJsonSafe(null)).toBe(null);
  });
});

describe('stringifyJsonSafe', () => {
  it('produces valid JSON that JSON.parse can round-trip a bigint offset through as a string', () => {
    const text = stringifyJsonSafe({ offset: 2n ** 63n - 1n });
    expect(() => JSON.parse(text)).not.toThrow();
    expect(JSON.parse(text)).toEqual({ offset: '9223372036854775807' });
  });

  it('never throws for a bare bigint, unlike JSON.stringify', () => {
    expect(() => JSON.stringify(5n)).toThrow(TypeError);
    expect(() => stringifyJsonSafe(5n)).not.toThrow();
  });
});
