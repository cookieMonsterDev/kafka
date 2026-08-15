import { describe, expect, it } from 'vitest';
import { AlterPartitionReassignments } from './index.js';

describe('protocol/requests/alter-partition-reassignments', () => {
  it('implements version 0 only', () => {
    expect(AlterPartitionReassignments.versions).toEqual([0]);
  });
});
