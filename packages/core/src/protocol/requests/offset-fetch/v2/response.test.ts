import { describe, expect, it } from 'vitest';
import v2ResponseFixture from '../fixtures/v2-response.json' with { type: 'json' };
import { offsetFetchResponseV2 } from './response.js';

describe('protocol/requests/offset-fetch/v2/response', () => {
  it('decodes a real fixture, including the top-level error_code', async () => {
    const data = await offsetFetchResponseV2.decode(Buffer.from(v2ResponseFixture.data));
    expect(data).toEqual({
      responses: [
        {
          topic: 'test-topic-2cbbd6e6362f1a638c94',
          partitions: [{ partition: 0, offset: -1n, metadata: '', errorCode: 0 }],
        },
      ],
      errorCode: 0,
    });
    await expect(offsetFetchResponseV2.parse(data)).resolves.toBe(data);
  });

  it('throws on a top-level failure before checking partitions', async () => {
    const data = {
      responses: [{ topic: 't', partitions: [{ partition: 0, offset: -1n, metadata: null, errorCode: 0 }] }],
      errorCode: 15,
    };
    await expect(offsetFetchResponseV2.parse(data)).rejects.toThrow();
  });
});
