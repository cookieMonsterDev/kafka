import { describe, expect, it } from 'vitest';
import v1RequestFixture from '../fixtures/v1-request.json' with { type: 'json' };
import { withTopicDefaults } from '../v2/request';
import { createTopicsRequestV1 } from './request';

describe('protocol/requests/create-topics/v1/request', () => {
  it('encodes a real fixture with validateOnly', async () => {
    const encoder = await createTopicsRequestV1({
      topics: withTopicDefaults([
        { topic: 'test-topic-c8d8ca3d95495c6b900d' },
        { topic: 'test-topic-050fb2e6aed13a954288' },
      ]),
      timeout: 5000,
      validateOnly: true,
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v1RequestFixture.data));
  });
});
