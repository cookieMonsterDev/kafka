import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { deleteTopicsResponseV0 } from './response';

describe('protocol/requests/delete-topics/v0/response', () => {
  it('decodes a real fixture, sorted by topic name', async () => {
    const data = await deleteTopicsResponseV0.decode(Buffer.from(v0ResponseFixture.data));

    expect(data).toEqual({
      topicErrors: [
        { topic: 'test-topic-bb9886eb859786ce646d', errorCode: 0 },
        { topic: 'test-topic-d41416601b422429db78', errorCode: 0 },
      ],
    });
    await expect(deleteTopicsResponseV0.parse(data)).resolves.toBeTruthy();
  });
});
