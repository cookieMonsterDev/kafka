import { describe, expect, it } from 'vitest';
import { KafkaNonRetriableError } from '../../../errors';
import { ListGroups } from './index';

describe('protocol/requests/list-groups', () => {
  it('implements versions 0 through 5', () => {
    expect(ListGroups.versions).toEqual([0, 1, 2, 3, 4, 5]);
  });

  it('sends the type filter on v5', () => {
    const { request } = ListGroups.protocol({ version: 5 })({ types: ['consumer'] });
    expect(request.apiVersion).toBe(5);
  });

  it.each([0, 1, 2, 3, 4])('rejects a type filter on v%i instead of listing every group', (version) => {
    expect(() => ListGroups.protocol({ version })({ types: ['consumer'] })).toThrow(KafkaNonRetriableError);
  });

  it.each([0, 4])('accepts an empty type filter on v%i', (version) => {
    expect(ListGroups.protocol({ version })({ types: [] }).request.apiVersion).toBe(version);
  });
});
