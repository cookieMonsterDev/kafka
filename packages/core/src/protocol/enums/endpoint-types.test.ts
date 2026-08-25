import { describe, expect, it } from 'vitest';
import { ENDPOINT_TYPES } from './endpoint-types';

describe('protocol/enums/endpoint-types', () => {
  it('matches the DescribeCluster endpoint_type wire values (KIP-919)', () => {
    expect(ENDPOINT_TYPES.UNKNOWN).toBe(0);
    expect(ENDPOINT_TYPES.BROKER).toBe(1);
    expect(ENDPOINT_TYPES.CONTROLLER).toBe(2);
    expect(Object.isFrozen(ENDPOINT_TYPES)).toBe(true);
  });
});
