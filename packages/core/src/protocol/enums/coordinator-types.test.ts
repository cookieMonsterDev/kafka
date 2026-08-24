import { describe, expect, it } from 'vitest';
import { COORDINATOR_TYPES } from './coordinator-types';

describe('protocol/enums/coordinator-types', () => {
  it('matches the FindCoordinator coordinator_type wire values', () => {
    expect(COORDINATOR_TYPES.GROUP).toBe(0);
    expect(COORDINATOR_TYPES.TRANSACTION).toBe(1);
    expect(Object.isFrozen(COORDINATOR_TYPES)).toBe(true);
  });
});
