import { describe, expect, it } from 'vitest';
import { HeartbeatMember } from './heartbeat-member';

const response = { memberId: 'member', memberEpoch: 3, heartbeatIntervalMs: 5_000 };

describe('consumer/membership/heartbeat-member', () => {
  it('sends the join fields until the broker accepts the member', () => {
    const member = new HeartbeatMember();
    member.startJoin();

    expect(member.memberId).not.toBeNull();
    expect(member.sendsJoinFields).toBe(true);

    member.applyResponse({ ...response, memberEpoch: 0 });
    expect(member.isJoined).toBe(false);
    expect(member.sendsJoinFields).toBe(true);

    member.applyResponse(response);
    expect(member.isJoined).toBe(true);
    expect(member.memberId).toBe('member');
    expect(member.sendsJoinFields).toBe(false);
  });

  it('rejoins under the same member id after a fence, and under a new one when the id is unknown', () => {
    const member = new HeartbeatMember();
    member.startJoin();
    member.applyResponse(response);

    member.reset();
    expect(member).toMatchObject({ memberId: 'member', memberEpoch: 0, isJoined: false, sendsJoinFields: true });

    member.reset({ forgetMemberId: true });
    expect(member.memberId).toBeNull();
  });

  it('uses the broker heartbeat interval once one arrives', () => {
    const member = new HeartbeatMember();
    expect(member.nextHeartbeatAt(1_000)).toBe(1_000);

    member.applyResponse(response);
    expect(member.nextHeartbeatAt(1_000)).toBe(member.lastHeartbeatAt + 5_000);
  });
});
