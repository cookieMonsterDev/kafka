import { describe, expect, it } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { resolveConfigResourceType } from './resource-type';

describe('resolveConfigResourceType', () => {
  it('resolves a lowercase name', () => {
    expect(resolveConfigResourceType('topic')).toBe(2);
  });

  it('resolves an uppercase name', () => {
    expect(resolveConfigResourceType('TOPIC')).toBe(2);
  });

  it('resolves a kebab-case multi-word name', () => {
    expect(resolveConfigResourceType('broker-logger')).toBe(8);
    expect(resolveConfigResourceType('client-metrics')).toBe(16);
  });

  it('resolves every selectable type', () => {
    expect(resolveConfigResourceType('broker')).toBe(4);
    expect(resolveConfigResourceType('group')).toBe(32);
  });

  it('resolves a raw numeric code', () => {
    expect(resolveConfigResourceType('2')).toBe(2);
  });

  it('rejects "unknown" even though it is a real (unselectable) code', () => {
    expect(() => resolveConfigResourceType('unknown')).toThrow(CliUsageError);
    expect(() => resolveConfigResourceType('0')).toThrow(CliUsageError);
  });

  it('rejects an unrecognized name, listing the valid choices', () => {
    expect(() => resolveConfigResourceType('bogus')).toThrow(/--type must be one of/);
  });

  it('rejects a numeric code that maps to no resource type', () => {
    expect(() => resolveConfigResourceType('999')).toThrow(CliUsageError);
  });
});
