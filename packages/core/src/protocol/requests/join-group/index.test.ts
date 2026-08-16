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

  describe('requestTimeout', () => {
    it('uses sessionTimeout plus network delay on v0', () => {
      const sessionTimeout = 30_000;
      const protocol = JoinGroup.protocol({ version: 0 })({
        groupId: 'test-group',
        sessionTimeout,
        memberId: '',
        protocolType: 'consumer',
        groupProtocols: [{ name: 'default' }],
      });
      expect(protocol.requestTimeout).toBe(sessionTimeout + 5000);
    });

    it('does not overflow MAX_SAFE_INTEGER when adding the network delay', () => {
      const protocol = JoinGroup.protocol({ version: 0 })({
        groupId: 'test-group',
        sessionTimeout: Number.MAX_SAFE_INTEGER,
        memberId: '',
        protocolType: 'consumer',
        groupProtocols: [{ name: 'default' }],
      });
      expect(protocol.requestTimeout).toBe(Number.MAX_SAFE_INTEGER);
    });

    it('uses rebalanceTimeout plus network delay on v1+', () => {
      const parameters = {
        groupId: 'test-group',
        sessionTimeout: 1,
        rebalanceTimeout: 30_000,
        memberId: '',
        protocolType: 'consumer',
        groupProtocols: [{ name: 'default' }],
      };

      for (const version of [1, 2, 3, 4, 5] as const) {
        const protocol = JoinGroup.protocol({ version })(parameters);
        expect(protocol.requestTimeout).toBe(35_000);
      }
    });
  });

  describe('logResponseError (v4+)', () => {
    it('does not log error responses when memberId is empty', () => {
      const protocol = JoinGroup.protocol({ version: 4 })({
        groupId: 'test-group',
        sessionTimeout: 1,
        rebalanceTimeout: 30_000,
        memberId: '',
        protocolType: 'consumer',
        groupProtocols: [{ name: 'default' }],
      });
      expect(protocol.logResponseError).toBe(false);
    });

    it('logs error responses when memberId is set', () => {
      const protocol = JoinGroup.protocol({ version: 5 })({
        groupId: 'test-group',
        sessionTimeout: 1,
        rebalanceTimeout: 30_000,
        memberId: 'member-id',
        protocolType: 'consumer',
        groupProtocols: [{ name: 'default' }],
      });
      expect(protocol.logResponseError).toBe(true);
    });
  });
});
