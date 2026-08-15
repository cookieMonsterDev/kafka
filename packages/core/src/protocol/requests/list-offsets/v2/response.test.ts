import { describe, expect, it } from 'vitest';
import v2ResponseFixture from '../fixtures/v2-response.json' with { type: 'json' };
import { listOffsetsResponseV2 } from './response.js';

describe('protocol/requests/list-offsets/v2/response', () => {
  it('decodes a real fixture, including throttleTime', async () => {
    const data = await listOffsetsResponseV2.decode(Buffer.from(v2ResponseFixture.data));

    expect(data).toEqual({
      throttleTime: 0,
      responses: [
        {
          topic: 'test-topic-84efe7aaafc3844b00c1-36211-2ee431b4-d40b-4df8-b2c8-fc9e33ab5c77',
          partitions: [{ partition: 0, errorCode: 0, timestamp: -1n, offset: 1n }],
        },
      ],
    });
    await expect(listOffsetsResponseV2.parse(data)).resolves.toBe(data);
  });
});
