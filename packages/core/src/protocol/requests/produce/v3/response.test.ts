import { describe, expect, it } from 'vitest';
import v3ResponseFixture from '../fixtures/v3-response.json' with { type: 'json' };
import { produceResponseV3 } from './response.js';

describe('protocol/requests/produce/v3/response', () => {
  it('decodes a real fixture', async () => {
    const data = await produceResponseV3.decode(Buffer.from(v3ResponseFixture.data));

    expect(data).toEqual({
      topics: [
        {
          topicName: 'test-topic-ebba68879c6f5081d8c2',
          partitions: [{ partition: 0, errorCode: 0, baseOffset: 0n, logAppendTime: -1n }],
        },
      ],
      throttleTime: 0,
    });
    await expect(produceResponseV3.parse(data)).resolves.toBeTruthy();
  });
});
