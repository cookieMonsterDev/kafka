import { describe, expect, it } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import {
  resolveAclOperationType,
  resolveAclPatternType,
  resolveAclPermissionType,
  resolveAclResourceType,
} from './enums';

describe('resolveAclResourceType', () => {
  it('resolves a lowercase name', () => {
    expect(resolveAclResourceType('topic')).toBe(2);
  });

  it('resolves an uppercase name', () => {
    expect(resolveAclResourceType('TOPIC')).toBe(2);
  });

  it('resolves a kebab-case multi-word name', () => {
    expect(resolveAclResourceType('transactional-id')).toBe(5);
    expect(resolveAclResourceType('delegation-token')).toBe(6);
  });

  it('resolves the "any" wildcard, unlike "unknown"', () => {
    expect(resolveAclResourceType('any')).toBe(1);
  });

  it('resolves a raw numeric code', () => {
    expect(resolveAclResourceType('4')).toBe(4);
  });

  it('rejects "unknown" even though it is a real (unselectable) code', () => {
    expect(() => resolveAclResourceType('unknown')).toThrow(CliUsageError);
    expect(() => resolveAclResourceType('0')).toThrow(CliUsageError);
  });

  it('rejects an unrecognized name, listing the valid choices', () => {
    expect(() => resolveAclResourceType('bogus')).toThrow(/--resource-type must be one of/);
  });

  it('rejects a numeric code that maps to no resource type', () => {
    expect(() => resolveAclResourceType('999')).toThrow(CliUsageError);
  });
});

describe('resolveAclPatternType', () => {
  it('resolves every selectable name', () => {
    expect(resolveAclPatternType('literal')).toBe(3);
    expect(resolveAclPatternType('prefixed')).toBe(4);
    expect(resolveAclPatternType('match')).toBe(2);
    expect(resolveAclPatternType('any')).toBe(1);
  });

  it('is case-insensitive', () => {
    expect(resolveAclPatternType('LITERAL')).toBe(3);
  });

  it('resolves a raw numeric code', () => {
    expect(resolveAclPatternType('3')).toBe(3);
  });

  it('rejects "unknown"', () => {
    expect(() => resolveAclPatternType('unknown')).toThrow(CliUsageError);
  });

  it('rejects an unrecognized value, listing the valid choices', () => {
    expect(() => resolveAclPatternType('bogus')).toThrow(/--pattern-type must be one of/);
  });
});

describe('resolveAclOperationType', () => {
  it('resolves every selectable name', () => {
    expect(resolveAclOperationType('read')).toBe(3);
    expect(resolveAclOperationType('write')).toBe(4);
    expect(resolveAclOperationType('describe-configs')).toBe(10);
    expect(resolveAclOperationType('idempotent-write')).toBe(12);
  });

  it('is case-insensitive', () => {
    expect(resolveAclOperationType('READ')).toBe(3);
  });

  it('resolves a raw numeric code', () => {
    expect(resolveAclOperationType('3')).toBe(3);
  });

  it('rejects "unknown"', () => {
    expect(() => resolveAclOperationType('unknown')).toThrow(CliUsageError);
  });

  it('rejects an unrecognized value, listing the valid choices', () => {
    expect(() => resolveAclOperationType('bogus')).toThrow(/--operation must be one of/);
  });

  it('rejects a numeric code that maps to no operation', () => {
    expect(() => resolveAclOperationType('999')).toThrow(CliUsageError);
  });
});

describe('resolveAclPermissionType', () => {
  it('resolves every selectable name', () => {
    expect(resolveAclPermissionType('allow')).toBe(3);
    expect(resolveAclPermissionType('deny')).toBe(2);
    expect(resolveAclPermissionType('any')).toBe(1);
  });

  it('is case-insensitive', () => {
    expect(resolveAclPermissionType('ALLOW')).toBe(3);
  });

  it('resolves a raw numeric code', () => {
    expect(resolveAclPermissionType('2')).toBe(2);
  });

  it('rejects "unknown"', () => {
    expect(() => resolveAclPermissionType('unknown')).toThrow(CliUsageError);
  });

  it('rejects an unrecognized value, listing the valid choices', () => {
    expect(() => resolveAclPermissionType('bogus')).toThrow(/--permission-type must be one of/);
  });
});
