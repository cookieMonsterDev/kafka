import { describe, expect, it } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { buildTopicConfig, parseReplicaAssignment } from './build-topic-config';

describe('parseReplicaAssignment', () => {
  it('parses one partition=replica,replica entry', () => {
    expect(parseReplicaAssignment(['0=1,2'])).toEqual([{ partition: 0, replicas: [1, 2] }]);
  });

  it('parses several entries', () => {
    expect(parseReplicaAssignment(['0=1,2', '1=2,3'])).toEqual([
      { partition: 0, replicas: [1, 2] },
      { partition: 1, replicas: [2, 3] },
    ]);
  });

  it('throws on a non-integer partition', () => {
    expect(() => parseReplicaAssignment(['x=1,2'])).toThrow(CliUsageError);
  });

  it('throws on a non-integer replica id', () => {
    expect(() => parseReplicaAssignment(['0=1,x'])).toThrow(CliUsageError);
  });

  it('throws when no replicas are listed', () => {
    expect(() => parseReplicaAssignment(['0='])).toThrow(CliUsageError);
  });
});

describe('buildTopicConfig', () => {
  it('builds a config with numPartitions and replicationFactor', () => {
    expect(buildTopicConfig('orders', { partitions: 3, replicationFactor: 2 })).toEqual({
      topic: 'orders',
      numPartitions: 3,
      replicationFactor: 2,
    });
  });

  it('builds a config with configEntries from a key=value record', () => {
    expect(buildTopicConfig('orders', { config: { 'retention.ms': '60000' } })).toEqual({
      topic: 'orders',
      configEntries: [{ name: 'retention.ms', value: '60000' }],
    });
  });

  it('builds a config with an explicit replica assignment', () => {
    expect(buildTopicConfig('orders', { replicaAssignment: ['0=1,2'] })).toEqual({
      topic: 'orders',
      replicaAssignment: [{ partition: 0, replicas: [1, 2] }],
    });
  });

  it('throws when partitions and replica-assignment are both given', () => {
    expect(() => buildTopicConfig('orders', { partitions: 3, replicaAssignment: ['0=1,2'] })).toThrow(CliUsageError);
  });

  it('throws when replication-factor and replica-assignment are both given', () => {
    expect(() => buildTopicConfig('orders', { replicationFactor: 2, replicaAssignment: ['0=1,2'] })).toThrow(
      CliUsageError,
    );
  });

  it('builds a bare config with no options', () => {
    expect(buildTopicConfig('orders', {})).toEqual({ topic: 'orders' });
  });
});
