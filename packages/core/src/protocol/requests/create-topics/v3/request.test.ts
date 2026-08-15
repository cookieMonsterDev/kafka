import { describe, expect, it } from 'vitest';
import v2RequestFixture from '../fixtures/v2-request.json' with { type: 'json' };
import { withTopicDefaults } from '../v2/request.js';
import { createTopicsRequestV3 } from './request.js';

describe('protocol/requests/create-topics/v3/request', () => {
  it('encodes to the same wire format as v2', async () => {
    const encoder = await createTopicsRequestV3({
      topics: withTopicDefaults([
        { topic: 'test-topic-fde67b5a797984ac0837-55492-1bf2f30a-cce8-403d-8897-6902a0b86fb0' },
        { topic: 'test-topic-3d6c53af2e0f9b1d1757-55492-cbde2344-d9d3-4ad7-b408-996cda13e6e5' },
      ]),
      validateOnly: false,
      timeout: 5000,
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v2RequestFixture.data));
  });
});
