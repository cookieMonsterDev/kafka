import { describe, expect, it } from 'vitest';
import { CliUsageError } from '../args/coerce';
import { decodeArgs } from './decode-args';

describe('decodeArgs', () => {
  it('decodes a bigint: string past 2^53', () => {
    expect(decodeArgs('bigint:9223372036854775807')).toBe(2n ** 63n - 1n);
  });

  it('throws CliUsageError on an invalid bigint: value', () => {
    expect(() => decodeArgs('bigint:not-a-number')).toThrow(CliUsageError);
  });

  it('decodes a base64: string to a Buffer', () => {
    const buffer = Buffer.from('hello');
    expect(decodeArgs(`base64:${buffer.toString('base64')}`)).toEqual(buffer);
  });

  it('decodes a uuid: string to its 16-byte Buffer', () => {
    const original = Buffer.from('0123456789abcdef', 'utf8');
    const hex = original.toString('hex');
    const uuid = `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
    expect(decodeArgs(`uuid:${uuid}`)).toEqual(original);
  });

  it('throws CliUsageError on a malformed uuid: value', () => {
    expect(() => decodeArgs('uuid:not-a-uuid')).toThrow(CliUsageError);
  });

  it('passes an ordinary string through untouched', () => {
    expect(decodeArgs('orders')).toBe('orders');
  });

  it('recurses through arrays and objects', () => {
    expect(decodeArgs({ topics: ['orders'], offset: 'bigint:5' })).toEqual({ topics: ['orders'], offset: 5n });
  });

  it('passes non-string primitives through untouched', () => {
    expect(decodeArgs(42)).toBe(42);
    expect(decodeArgs(true)).toBe(true);
    expect(decodeArgs(null)).toBe(null);
  });
});
