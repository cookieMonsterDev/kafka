import { describe, expect, it } from 'vitest';
import { CONNECTED_STATUS, CONNECTION_STATUS } from './connection-status';

describe('network/connection-status', () => {
  it('considers connected and disconnecting as connected states', () => {
    expect(CONNECTED_STATUS).toContain(CONNECTION_STATUS.CONNECTED);
    expect(CONNECTED_STATUS).toContain(CONNECTION_STATUS.DISCONNECTING);
  });

  it('does not consider disconnected a connected state', () => {
    expect(CONNECTED_STATUS).not.toContain(CONNECTION_STATUS.DISCONNECTED);
  });

  it('is frozen', () => {
    expect(Object.isFrozen(CONNECTION_STATUS)).toBe(true);
    expect(Object.isFrozen(CONNECTED_STATUS)).toBe(true);
  });
});
