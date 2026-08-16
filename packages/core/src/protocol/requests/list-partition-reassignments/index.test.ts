import { describe, expect, it } from 'vitest';
import { ListPartitionReassignments } from './index';

describe('protocol/requests/list-partition-reassignments', () => {
  it('implements version 0 only', () => {
    expect(ListPartitionReassignments.versions).toEqual([0]);
  });
});
