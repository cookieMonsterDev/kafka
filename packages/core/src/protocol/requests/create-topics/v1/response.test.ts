import { describe, expect, it } from 'vitest';
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' };
import { createTopicsResponseV1 } from './response';

describe('protocol/requests/create-topics/v1/response', () => {
  it('decodes a real fixture, sorted by topic name', async () => {
    const data = await createTopicsResponseV1.decode(Buffer.from(v1ResponseFixture.data));

    expect(data).toEqual({
      topicErrors: [
        {
          topic: 'test-topic-98abe97d412df76b4deb',
          errorCode: 0,
          errorMessage: null,
        },
      ],
    });
    await expect(createTopicsResponseV1.parse(data)).resolves.toBeTruthy();
  });
});
