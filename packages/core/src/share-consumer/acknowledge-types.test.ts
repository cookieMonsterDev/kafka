import { describe, expect, it } from 'vitest';
import { SHARE_ACKNOWLEDGE_TYPE } from './acknowledge-types';

describe('share-consumer/acknowledge-types', () => {
  it('matches the KIP-932 ShareAcknowledge type codes', () => {
    expect(SHARE_ACKNOWLEDGE_TYPE.GAP).toBe(0);
    expect(SHARE_ACKNOWLEDGE_TYPE.ACCEPT).toBe(1);
    expect(SHARE_ACKNOWLEDGE_TYPE.RELEASE).toBe(2);
    expect(SHARE_ACKNOWLEDGE_TYPE.REJECT).toBe(3);
    expect(SHARE_ACKNOWLEDGE_TYPE.RENEW).toBe(4);
    expect(Object.isFrozen(SHARE_ACKNOWLEDGE_TYPE)).toBe(true);
  });
});
