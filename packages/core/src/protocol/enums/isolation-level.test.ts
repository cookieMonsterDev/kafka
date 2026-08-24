import { describe, expect, it } from 'vitest';
import { ISOLATION_LEVEL } from './isolation-level';

describe('protocol/enums/isolation-level', () => {
  it('matches the Fetch isolation.level wire values', () => {
    expect(ISOLATION_LEVEL.READ_UNCOMMITTED).toBe(0);
    expect(ISOLATION_LEVEL.READ_COMMITTED).toBe(1);
    expect(Object.isFrozen(ISOLATION_LEVEL)).toBe(true);
  });
});
