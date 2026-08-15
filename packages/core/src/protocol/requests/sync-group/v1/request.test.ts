import { describe, expect, it } from 'vitest';
import v1MemberAssignmentFixture from '../fixtures/v1-member-assignment.json' with { type: 'json' };
import v1RequestFixture from '../fixtures/v1-request.json' with { type: 'json' };
import { syncGroupRequestV1 } from './request.js';

describe('protocol/requests/sync-group/v1/request', () => {
  it('encodes to match the real fixture', async () => {
    const memberId =
      'test-d44f97e7d1a0622387a1-24495-d057a55d-fb7c-446d-98b7-3a3a8dff7944-1f460f6f-bf82-4448-9c18-09b0d7eaceb6';
    const definition = syncGroupRequestV1({
      groupId: 'consumer-group-id-e15bd537f491e89484f1-24495-5c083268-3a66-4366-8afc-2c429edeb6af',
      generationId: 1,
      memberId,
      groupAssignment: [{ memberId, memberAssignment: Buffer.from(v1MemberAssignmentFixture.data) }],
    });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v1RequestFixture.data));
  });
});
