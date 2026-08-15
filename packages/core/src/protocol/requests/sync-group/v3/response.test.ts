import { describe, expect, it } from 'vitest';
import v1MemberAssignmentFixture from '../fixtures/v1-member-assignment.json' with { type: 'json' };
import v3ResponseFixture from '../fixtures/v3-response.json' with { type: 'json' };
import { syncGroupResponseV3 } from './response.js';

describe('protocol/requests/sync-group/v3/response', () => {
  it('decodes a real fixture (identical wire format to v2)', async () => {
    const data = await syncGroupResponseV3.decode(Buffer.from(v3ResponseFixture.data));
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      memberAssignment: Buffer.from(v1MemberAssignmentFixture.data),
    });
    await expect(syncGroupResponseV3.parse(data)).resolves.toBeTruthy();
  });
});
