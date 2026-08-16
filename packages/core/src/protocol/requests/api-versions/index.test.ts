import { describe, expect, it } from 'vitest';
import { ApiVersions } from './index';

describe('protocol/requests/api-versions', () => {
  it('implements versions 0 through 3', () => {
    expect(ApiVersions.versions).toEqual([0, 1, 2, 3]);
  });

  it('logs response errors on v0 only', () => {
    expect(ApiVersions.protocol({ version: 0 })({}).logResponseError).toBe(true);
    expect(ApiVersions.protocol({ version: 1 })({}).logResponseError).toBe(false);
    expect(ApiVersions.protocol({ version: 2 })({}).logResponseError).toBe(false);
    expect(ApiVersions.protocol({ version: 3 })({}).logResponseError).toBe(false);
  });

  it('throws for an unimplemented version', () => {
    expect(() => ApiVersions.protocol({ version: 99 })).toThrow(/Invariant violated/);
  });
});
