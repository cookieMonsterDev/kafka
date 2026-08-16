import { describe, expect, it } from 'vitest';
import v3ResponseFixture from '../fixtures/v3-response.json' with { type: 'json' };
import { offsetFetchResponseV3 } from './response';

describe('protocol/requests/offset-fetch/v3/response', () => {
  it('decodes a real fixture, including throttleTime and the top-level error_code', async () => {
    const data = await offsetFetchResponseV3.decode(Buffer.from(v3ResponseFixture.data));
    expect(data).toEqual({
      throttleTime: 0,
      responses: [
        {
          topic: 'test-topic-df48241c4bf2fca9d16b-20117-aff9b64c-69a2-4456-be7b-de5bcd78984e',
          partitions: [{ partition: 0, offset: -1n, metadata: '', errorCode: 0 }],
        },
      ],
      errorCode: 0,
    });
    await expect(offsetFetchResponseV3.parse(data)).resolves.toBe(data);
  });
});
