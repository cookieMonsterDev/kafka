import { describe, expect, it } from 'vitest';
import { SyncGroup } from './index';
import { syncGroupRequestV5 } from './v5/request';

describe('protocol/requests/sync-group', () => {
  it('implements versions 0 through 5', () => {
    expect(SyncGroup.versions).toEqual([0, 1, 2, 3, 4, 5]);
  });

  it('defaults groupInstanceId to null on v3 when omitted', () => {
    const { request } = SyncGroup.protocol({ version: 3 })({
      groupId: 'g',
      generationId: 1,
      memberId: 'm',
      groupAssignment: [],
    });
    expect(request.apiVersion).toBe(3);
  });

  it('defaults groupInstanceId to null on v4 when omitted', () => {
    const { request } = SyncGroup.protocol({ version: 4 })({
      groupId: 'g',
      generationId: 1,
      memberId: 'm',
      groupAssignment: [],
    });
    expect(request.apiVersion).toBe(4);
  });

  it('defaults protocolType and protocolName to null on v5 when omitted', async () => {
    const { request } = SyncGroup.protocol({ version: 5 })({
      groupId: 'g',
      generationId: 1,
      memberId: 'm',
      groupAssignment: [],
    });
    const expected = await syncGroupRequestV5({
      groupId: 'g',
      generationId: 1,
      memberId: 'm',
      groupInstanceId: null,
      protocolType: null,
      protocolName: null,
      groupAssignment: [],
    }).encode();
    expect(request.apiVersion).toBe(5);
    expect((await request.encode()).buffer).toEqual(expected.buffer);
  });
});
