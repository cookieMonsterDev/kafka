import { describe, expect, it } from 'vitest';
import v0ResponseEmptyMemberAssignmentFixture from '../fixtures/v0-response-empty-member-assignment.json' with { type: 'json' };
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { syncGroupResponseV0 } from './response.js';

describe('protocol/requests/sync-group/v0/response', () => {
  it('decodes a real fixture', async () => {
    const data = await syncGroupResponseV0.decode(Buffer.from(v0ResponseFixture.data));
    expect(data).toEqual({
      errorCode: 0,
      memberAssignment: Buffer.from(JSON.stringify({ 'topic-test': [2, 5, 4, 1, 3, 0] })),
    });
    await expect(syncGroupResponseV0.parse(data)).resolves.toBeTruthy();
  });

  it('handles an empty member assignment', async () => {
    const data = await syncGroupResponseV0.decode(Buffer.from(v0ResponseEmptyMemberAssignmentFixture.data));
    expect(data).toEqual({ errorCode: 0, memberAssignment: Buffer.from([]) });
  });
});
