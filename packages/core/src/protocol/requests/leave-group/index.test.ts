import { describe, expect, it } from 'vitest';
import { LeaveGroup } from './index';
import { leaveGroupRequestV3 } from './v3/request';

describe('protocol/requests/leave-group', () => {
  it('implements versions 0 through 3', () => {
    expect(LeaveGroup.versions).toEqual([0, 1, 2, 3]);
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
});
