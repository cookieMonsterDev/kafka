import { describe, expect, it } from 'vitest';
import { API_KEYS, apiKeyName } from './api-keys';

describe('protocol/requests/api-keys', () => {
  it('assigns the well-known Kafka protocol api keys', () => {
    expect(API_KEYS.Produce).toBe(0);
    expect(API_KEYS.Fetch).toBe(1);
    expect(API_KEYS.Metadata).toBe(3);
    expect(API_KEYS.ApiVersions).toBe(18);
    expect(API_KEYS.ElectLeaders).toBe(43);
    expect(API_KEYS.IncrementalAlterConfigs).toBe(44);
    expect(API_KEYS.ListPartitionReassignments).toBe(46);
    expect(API_KEYS.OffsetDelete).toBe(47);
    expect(API_KEYS.DescribeUserScramCredentials).toBe(50);
    expect(API_KEYS.AlterUserScramCredentials).toBe(51);
    expect(API_KEYS.UpdateFeatures).toBe(57);
    expect(API_KEYS.DescribeCluster).toBe(60);
    expect(API_KEYS.DescribeProducers).toBe(61);
    expect(API_KEYS.DescribeTransactions).toBe(65);
    expect(API_KEYS.ListTransactions).toBe(66);
    expect(API_KEYS.ListConfigResources).toBe(74);
  });

  it('is frozen', () => {
    expect(() => {
      (API_KEYS as { Produce: number }).Produce = 999;
    }).toThrow();
  });

  it('resolves a name back from a key', () => {
    expect(apiKeyName(18)).toBe('ApiVersions');
    expect(apiKeyName(43)).toBe('ElectLeaders');
    expect(apiKeyName(47)).toBe('OffsetDelete');
    expect(apiKeyName(50)).toBe('DescribeUserScramCredentials');
    expect(apiKeyName(9999)).toBeUndefined();
  });
});
