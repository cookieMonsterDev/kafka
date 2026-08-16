import { describe, expect, it } from 'vitest';
import { LeaveGroup } from './index';
import { leaveGroupRequestV3 } from './v3/request';
import { leaveGroupRequestV4 } from './v4/request';
import { leaveGroupRequestV5 } from './v5/request';

describe('protocol/requests/leave-group', () => {
  it('implements versions 0 through 5', () => {
    expect(LeaveGroup.versions).toEqual([0, 1, 2, 3, 4, 5]);
  });

  it('builds a single-member request pre-v3 from memberId', async () => {
    const { request } = LeaveGroup.protocol({ version: 0 })({ groupId: 'g', memberId: 'm' });
    expect(request.apiVersion).toBe(0);
  });

  it('builds a batch request on v3 from memberId alone (wrapped into a single-element batch)', async () => {
    const { request } = LeaveGroup.protocol({ version: 3 })({ groupId: 'g', memberId: 'm' });
    const expected = await leaveGroupRequestV3({
      groupId: 'g',
      members: [{ memberId: 'm', groupInstanceId: null }],
    }).encode();
    expect((await request.encode()).buffer).toEqual(expected.buffer);
  });

  it('builds a batch request on v4 from memberId alone', async () => {
    const { request } = LeaveGroup.protocol({ version: 4 })({ groupId: 'g', memberId: 'm' });
    const expected = await leaveGroupRequestV4({
      groupId: 'g',
      members: [{ memberId: 'm', groupInstanceId: null }],
    }).encode();
    expect((await request.encode()).buffer).toEqual(expected.buffer);
  });

  it('builds a batch request on v5 from memberId alone, defaulting reason to null', async () => {
    const { request } = LeaveGroup.protocol({ version: 5 })({ groupId: 'g', memberId: 'm' });
    const expected = await leaveGroupRequestV5({
      groupId: 'g',
      members: [{ memberId: 'm', groupInstanceId: null, reason: null }],
    }).encode();
    expect((await request.encode()).buffer).toEqual(expected.buffer);
  });

  it('forwards groupInstanceId on v3 when provided', async () => {
    const { request } = LeaveGroup.protocol({ version: 3 })({
      groupId: 'g',
      memberId: 'm',
      groupInstanceId: 'static-1',
    });
    const expected = await leaveGroupRequestV3({
      groupId: 'g',
      members: [{ memberId: 'm', groupInstanceId: 'static-1' }],
    }).encode();
    expect((await request.encode()).buffer).toEqual(expected.buffer);
  });

  it('forwards member reason on v5 when provided', async () => {
    const { request } = LeaveGroup.protocol({ version: 5 })({
      groupId: 'g',
      members: [{ memberId: 'm', groupInstanceId: null, reason: 'shutdown' }],
    });
    const expected = await leaveGroupRequestV5({
      groupId: 'g',
      members: [{ memberId: 'm', groupInstanceId: null, reason: 'shutdown' }],
    }).encode();
    expect((await request.encode()).buffer).toEqual(expected.buffer);
  });
});
