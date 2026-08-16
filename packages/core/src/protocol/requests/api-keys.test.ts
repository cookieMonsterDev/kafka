import { describe, expect, it } from 'vitest';
import { API_KEYS, apiKeyName } from './api-keys';

describe('protocol/requests/api-keys', () => {
  it('assigns the well-known Kafka protocol api keys', () => {
    expect(API_KEYS.Produce).toBe(0);
    expect(API_KEYS.Fetch).toBe(1);
    expect(API_KEYS.Metadata).toBe(3);
    expect(API_KEYS.ApiVersions).toBe(18);
    expect(API_KEYS.ListPartitionReassignments).toBe(46);
  });

  it('is frozen', () => {
    expect(() => {
      (API_KEYS as { Produce: number }).Produce = 999;
    }).toThrow();
  });

  it('resolves a name back from a key', () => {
    expect(apiKeyName(18)).toBe('ApiVersions');
    expect(apiKeyName(9999)).toBeUndefined();
  });
});
