import { describe, expect, it } from 'vitest';
import v2ResponseFixture from '../fixtures/v2-response.json' with { type: 'json' };
import { createTopicsResponseV2 } from './response';

describe('protocol/requests/create-topics/v2/response', () => {
  it('decodes a real fixture, sorted by topic name', async () => {
    const data = await createTopicsResponseV2.decode(Buffer.from(v2ResponseFixture.data));

    expect(data).toEqual({
      throttleTime: 0,
      topicErrors: [
        {
          topic: 'test-topic-3d6c53af2e0f9b1d1757-55492-cbde2344-d9d3-4ad7-b408-996cda13e6e5',
          errorCode: 0,
          errorMessage: null,
        },
        {
          topic: 'test-topic-fde67b5a797984ac0837-55492-1bf2f30a-cce8-403d-8897-6902a0b86fb0',
          errorCode: 0,
          errorMessage: null,
        },
      ],
    });
    await expect(createTopicsResponseV2.parse(data)).resolves.toBeTruthy();
  });
});
