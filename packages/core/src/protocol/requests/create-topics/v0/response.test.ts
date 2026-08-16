import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { createTopicsResponseV0 } from './response';

describe('protocol/requests/create-topics/v0/response', () => {
  it('decodes a real fixture, sorted by topic name', async () => {
    const data = await createTopicsResponseV0.decode(Buffer.from(v0ResponseFixture.data));

    expect(data).toEqual({
      topicErrors: [
        { errorCode: 0, topic: 'test-topic-050fb2e6aed13a954288' },
        { errorCode: 0, topic: 'test-topic-c8d8ca3d95495c6b900d' },
      ],
    });
    await expect(createTopicsResponseV0.parse(data)).resolves.toBeTruthy();
  });
});
