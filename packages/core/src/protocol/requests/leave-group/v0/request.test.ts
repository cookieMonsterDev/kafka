import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { leaveGroupRequestV0 } from './request.js';

describe('protocol/requests/leave-group/v0/request', () => {
  it('encodes to match the real fixture', async () => {
    const definition = leaveGroupRequestV0({
      groupId: 'consumer-group-id-64fbf5dce5065868aa8f',
      memberId: 'test-45eb7a4239f548578e8b-b2b08fa3-b887-4719-b9e1-391ec944b53f',
    });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
