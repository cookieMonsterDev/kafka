import { describe, expect, it } from 'vitest';
import v2RequestFixture from '../fixtures/v2-request.json' with { type: 'json' };
import { createTopicsRequestV3 } from '../v3/request';
import { withTopicDefaults } from '../v2/request';
import { createTopicsRequestV4 } from './request';

const payload = {
  topics: withTopicDefaults([
    { topic: 'test-topic-fde67b5a797984ac0837-55492-1bf2f30a-cce8-403d-8897-6902a0b86fb0' },
    { topic: 'test-topic-3d6c53af2e0f9b1d1757-55492-cbde2344-d9d3-4ad7-b408-996cda13e6e5' },
  ]),
  validateOnly: false,
  timeout: 5000,
};

describe('protocol/requests/create-topics/v4/request', () => {
  it('encodes identically to v3, wire-for-wire', async () => {
    const definition = createTopicsRequestV4(payload);
    expect(definition.apiVersion).toBe(4);

    const encoder = await definition.encode();
    const v3 = await createTopicsRequestV3(payload).encode();
    expect(encoder.buffer).toEqual(v3.buffer);
    expect(encoder.buffer).toEqual(Buffer.from(v2RequestFixture.data));
  });
});
