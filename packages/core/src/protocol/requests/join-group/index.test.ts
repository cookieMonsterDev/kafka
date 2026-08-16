import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { JoinGroup } from './index';

describe('protocol/requests/join-group', () => {
  it('implements versions 0 through 5', () => {
    expect(JoinGroup.versions).toEqual([0, 1, 2, 3, 4, 5]);
  });

  it('defaults rebalanceTimeout to sessionTimeout on v1+', async () => {
    const { request } = JoinGroup.protocol({ version: 1 })({
      groupId: 'g',
      sessionTimeout: 12345,
      memberId: '',
      protocolType: 'consumer',
      groupProtocols: [{ name: 'p' }],
    });
    const encoder = await request.encode();
    const decoder = new Decoder(encoder.buffer);
    expect(decoder.readString()).toBe('g');
    expect(decoder.readInt32()).toBe(12345); // sessionTimeout
    expect(decoder.readInt32()).toBe(12345); // rebalanceTimeout, defaulted
  });

  it('carries apiVersion 5', () => {
    const { request } = JoinGroup.protocol({ version: 5 })({
      groupId: 'g',
      sessionTimeout: 1,
      rebalanceTimeout: 1,
      memberId: '',
      protocolType: 'consumer',
      groupProtocols: [{ name: 'p' }],
    });
    expect(request.apiVersion).toBe(5);
  });
});
