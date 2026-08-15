import { describe, expect, it } from 'vitest';
import v1MemberAssignmentFixture from '../fixtures/v1-member-assignment.json' with { type: 'json' };
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' };
import { syncGroupResponseV1 } from './response.js';

describe('protocol/requests/sync-group/v1/response', () => {
  it('decodes a real fixture', async () => {
    const data = await syncGroupResponseV1.decode(Buffer.from(v1ResponseFixture.data));
    expect(data).toEqual({
      throttleTime: 0,
      errorCode: 0,
      memberAssignment: Buffer.from(v1MemberAssignmentFixture.data),
    });
    await expect(syncGroupResponseV1.parse(data)).resolves.toBeTruthy();
  });
});
