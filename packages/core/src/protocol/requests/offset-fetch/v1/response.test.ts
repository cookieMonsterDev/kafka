import { describe, expect, it } from 'vitest';
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' };
import { offsetFetchResponseV1 } from './response.js';

describe('protocol/requests/offset-fetch/v1/response', () => {
  it('decodes a real fixture', async () => {
    const data = await offsetFetchResponseV1.decode(Buffer.from(v1ResponseFixture.data));
    expect(data).toEqual({
      responses: [
        {
          topic: 'test-topic-9f9b074057acd4335946',
          partitions: [{ partition: 0, offset: -1n, metadata: '', errorCode: 0 }],
        },
      ],
    });
    await expect(offsetFetchResponseV1.parse(data)).resolves.toBe(data);
  });

  it('throws on the first partition-level failure', async () => {
    const data = {
      responses: [{ topic: 't', partitions: [{ partition: 0, offset: -1n, metadata: null, errorCode: 16 }] }],
    };
    await expect(offsetFetchResponseV1.parse(data)).rejects.toThrow();
  });
});
