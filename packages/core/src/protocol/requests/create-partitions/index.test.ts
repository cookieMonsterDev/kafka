import { describe, expect, it } from 'vitest';
import { CreatePartitions } from './index';

describe('protocol/requests/create-partitions', () => {
  it('implements versions 0 through 3', () => {
    expect(CreatePartitions.versions).toEqual([0, 1, 2, 3]);
  });
});
