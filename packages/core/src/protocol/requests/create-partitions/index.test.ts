import { describe, expect, it } from 'vitest';
import { CreatePartitions } from './index.js';

describe('protocol/requests/create-partitions', () => {
  it('implements versions 0 through 1', () => {
    expect(CreatePartitions.versions).toEqual([0, 1]);
  });
});
