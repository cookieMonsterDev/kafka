import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { heartbeatRequestV0 } from './request.js';

describe('protocol/requests/heartbeat/v0/request', () => {
  it('encodes to match the real fixture', async () => {
    const definition = heartbeatRequestV0({
      groupId: 'consumer-group-id-ba8da1f6117562ed5615',
      groupGenerationId: 1,
      memberId: 'test-169232b069c4a377bc4b-040f5f1a-a469-4062-9d36-bd803d8d6767',
    });
    const encoder = await definition.encode();

    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
