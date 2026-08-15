import { describe, expect, it } from 'vitest';
import v1MemberAssignmentFixture from '../fixtures/v1-member-assignment.json' with { type: 'json' };
import v2ResponseFixture from '../fixtures/v2-response.json' with { type: 'json' };
import { syncGroupResponseV2 } from './response.js';

describe('protocol/requests/sync-group/v2/response', () => {
  it('decodes the v1 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await syncGroupResponseV2.decode(Buffer.from(v2ResponseFixture.data));
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      memberAssignment: Buffer.from(v1MemberAssignmentFixture.data),
    });
    await expect(syncGroupResponseV2.parse(data)).resolves.toBeTruthy();
  });
});
