import { describe, expect, it } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { toAlterQuotaEntity, toDescribeQuotaComponents } from './entity';

describe('toAlterQuotaEntity', () => {
  it('rejects an undefined entity map', () => {
    expect(() => toAlterQuotaEntity(undefined)).toThrow(CliUsageError);
  });

  it('rejects an empty entity map', () => {
    expect(() => toAlterQuotaEntity({})).toThrow(CliUsageError);
  });

  it('builds one entry per entity type', () => {
    expect(toAlterQuotaEntity({ user: 'alice', 'client-id': 'orders-producer' })).toEqual([
      { entityType: 'user', entityName: 'alice' },
      { entityType: 'client-id', entityName: 'orders-producer' },
    ]);
  });

  it('maps an empty name to the cluster default (null)', () => {
    expect(toAlterQuotaEntity({ user: '' })).toEqual([{ entityType: 'user', entityName: null }]);
  });
});

describe('toDescribeQuotaComponents', () => {
  it('returns an empty array when nothing is given', () => {
    expect(toDescribeQuotaComponents(undefined, undefined)).toEqual([]);
  });

  it('builds an exact-match component for a named entity', () => {
    expect(toDescribeQuotaComponents({ user: 'alice' }, undefined)).toEqual([
      { entityType: 'user', matchType: 0, match: 'alice' },
    ]);
  });

  it('builds a default-match component for an empty name', () => {
    expect(toDescribeQuotaComponents({ user: '' }, undefined)).toEqual([
      { entityType: 'user', matchType: 1, match: null },
    ]);
  });

  it('builds an any-specified-name component from --entity-any', () => {
    expect(toDescribeQuotaComponents(undefined, ['client-id'])).toEqual([
      { entityType: 'client-id', matchType: 2, match: null },
    ]);
  });

  it('combines --entity and --entity-any components', () => {
    expect(toDescribeQuotaComponents({ user: 'alice' }, ['client-id'])).toEqual([
      { entityType: 'user', matchType: 0, match: 'alice' },
      { entityType: 'client-id', matchType: 2, match: null },
    ]);
  });
});
