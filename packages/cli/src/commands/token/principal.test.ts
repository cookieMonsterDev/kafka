import { describe, expect, it } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { parsePrincipalFlag, parsePrincipalFlags } from './principal';

describe('parsePrincipalFlag', () => {
  it('splits "PrincipalType:name" on the first colon', () => {
    expect(parsePrincipalFlag('User:alice', 'owner')).toEqual({ principalType: 'User', name: 'alice' });
  });

  it('keeps everything after the first colon as the name', () => {
    expect(parsePrincipalFlag('User:alice:staging', 'owner')).toEqual({ principalType: 'User', name: 'alice:staging' });
  });

  it('rejects a value with no colon', () => {
    expect(() => parsePrincipalFlag('alice', 'owner')).toThrow(CliUsageError);
    expect(() => parsePrincipalFlag('alice', 'owner')).toThrow(/--owner must be "PrincipalType:name"/);
  });

  it('rejects a value with an empty principal type', () => {
    expect(() => parsePrincipalFlag(':alice', 'owner')).toThrow(CliUsageError);
  });

  it('rejects a value with an empty name', () => {
    expect(() => parsePrincipalFlag('User:', 'owner')).toThrow(CliUsageError);
  });
});

describe('parsePrincipalFlags', () => {
  it('returns undefined when given undefined', () => {
    expect(parsePrincipalFlags(undefined, 'renewer')).toBeUndefined();
  });

  it('parses every entry', () => {
    expect(parsePrincipalFlags(['User:alice', 'User:bob'], 'renewer')).toEqual([
      { principalType: 'User', name: 'alice' },
      { principalType: 'User', name: 'bob' },
    ]);
  });

  it('propagates a parse failure from any entry', () => {
    expect(() => parsePrincipalFlags(['User:alice', 'bogus'], 'renewer')).toThrow(CliUsageError);
  });
});
