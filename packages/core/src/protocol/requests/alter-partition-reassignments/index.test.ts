import { describe, expect, it } from 'vitest';
import { KafkaNonRetriableError } from '../../../errors';
import { AlterPartitionReassignments } from './index';

const topics = [{ topic: 'orders', partitionAssignment: [{ partition: 0, replicas: [1, 2] }] }];

describe('protocol/requests/alter-partition-reassignments', () => {
  it('implements versions 0 and 1', () => {
    expect(AlterPartitionReassignments.versions).toEqual([0, 1]);
  });

  it('builds a request for the requested version', () => {
    expect(AlterPartitionReassignments.protocol({ version: 0 })({ topics }).request.apiVersion).toBe(0);
    expect(AlterPartitionReassignments.protocol({ version: 1 })({ topics }).request.apiVersion).toBe(1);
  });

  it('rejects allowReplicationFactorChange: false on v0 instead of silently allowing the change', () => {
    expect(() =>
      AlterPartitionReassignments.protocol({ version: 0 })({ topics, allowReplicationFactorChange: false }),
    ).toThrow(KafkaNonRetriableError);
  });

  it('accepts allowReplicationFactorChange: true on v0, which matches its behavior', () => {
    const { request } = AlterPartitionReassignments.protocol({ version: 0 })({
      topics,
      allowReplicationFactorChange: true,
    });
    expect(request.apiVersion).toBe(0);
  });
});
