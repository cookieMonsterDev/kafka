import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { joinGroupRequestV0, withDefaultMetadata } from './request.js';

describe('protocol/requests/join-group/v0/request', () => {
  it('encodes to match the real fixture, defaulting metadata to an empty buffer', async () => {
    const definition = joinGroupRequestV0({
      groupId: 'test-group',
      sessionTimeout: 30000,
      memberId: '',
      protocolType: 'consumer',
      groupProtocols: withDefaultMetadata([{ name: 'default' }]),
    });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
