import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { withTopicDefaults } from '../v2/request';
import { createTopicsRequestV0 } from './request';

describe('protocol/requests/create-topics/v0/request', () => {
  it('encodes a real fixture', async () => {
    const encoder = await createTopicsRequestV0({
      topics: withTopicDefaults([
        { topic: 'test-topic-c8d8ca3d95495c6b900d' },
        { topic: 'test-topic-050fb2e6aed13a954288' },
      ]),
      timeout: 5000,
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
