import { describe, expect, it } from 'vitest';
import v1RequestFixture from '../fixtures/v1-request.json' with { type: 'json' };
import { heartbeatRequestV1 } from './request.js';

describe('protocol/requests/heartbeat/v1/request', () => {
  it('encodes to match the real fixture', async () => {
    const definition = heartbeatRequestV1({
      groupId: 'consumer-group-id-4c456000151f094b600d-26762-fd6a6ae7-3f66-408e-802e-d261d6983d0d',
      groupGenerationId: 1,
      memberId:
        'test-14da1b41ac688a6dcb78-26762-4dac8e12-dc28-4db2-8456-95bc6c1589bb-7bad1e84-c2de-4cc6-8071-badb27c86166',
    });
    const encoder = await definition.encode();

    expect(encoder.buffer).toEqual(Buffer.from(v1RequestFixture.data));
  });
});
