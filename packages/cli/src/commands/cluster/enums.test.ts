import { describe, expect, it } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { resolveElectionType, resolveFeatureUpgradeType } from './enums';

describe('resolveFeatureUpgradeType', () => {
  it('resolves a case-insensitive, kebab-cased name', () => {
    expect(resolveFeatureUpgradeType('upgrade')).toBe(1);
    expect(resolveFeatureUpgradeType('SAFE-DOWNGRADE')).toBe(2);
    expect(resolveFeatureUpgradeType('unsafe-downgrade')).toBe(3);
  });

  it('resolves a raw numeric code', () => {
    expect(resolveFeatureUpgradeType('2')).toBe(2);
  });

  it('rejects an unrecognized value, naming every valid choice', () => {
    expect(() => resolveFeatureUpgradeType('bogus')).toThrow(CliUsageError);
    expect(() => resolveFeatureUpgradeType('bogus')).toThrow(/upgrade, safe-downgrade, unsafe-downgrade/);
  });

  it('rejects a numeric code outside the known set', () => {
    expect(() => resolveFeatureUpgradeType('99')).toThrow(CliUsageError);
  });
});

describe('resolveElectionType', () => {
  it('resolves "preferred" and "unclean" by name', () => {
    expect(resolveElectionType('preferred')).toBe(0);
    expect(resolveElectionType('UNCLEAN')).toBe(1);
  });

  it('resolves a raw numeric code', () => {
    expect(resolveElectionType('0')).toBe(0);
    expect(resolveElectionType('1')).toBe(1);
  });

  it('rejects an unrecognized value', () => {
    expect(() => resolveElectionType('bogus')).toThrow(CliUsageError);
  });
});
