import { describe, expect, it } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { resolveScramMechanism } from './enums';

describe('resolveScramMechanism', () => {
  it('resolves every selectable name', () => {
    expect(resolveScramMechanism('scram-sha-256')).toBe(1);
    expect(resolveScramMechanism('scram-sha-512')).toBe(2);
  });

  it('is case-insensitive', () => {
    expect(resolveScramMechanism('SCRAM-SHA-256')).toBe(1);
  });

  it('resolves a raw numeric code', () => {
    expect(resolveScramMechanism('2')).toBe(2);
  });

  it('rejects "unknown" even though it is a real (unselectable) code', () => {
    expect(() => resolveScramMechanism('unknown')).toThrow(CliUsageError);
    expect(() => resolveScramMechanism('0')).toThrow(CliUsageError);
  });

  it('rejects an unrecognized name, listing the valid choices', () => {
    expect(() => resolveScramMechanism('bogus')).toThrow(/--mechanism must be one of/);
  });

  it('rejects a numeric code that maps to no mechanism', () => {
    expect(() => resolveScramMechanism('999')).toThrow(CliUsageError);
  });
});
