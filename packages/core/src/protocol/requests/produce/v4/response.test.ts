import { describe, expect, it } from 'vitest';
import v4ResponseFixture from '../fixtures/v4-response.json' with { type: 'json' };
import { produceResponseV4 } from './response.js';

describe('protocol/requests/produce/v4/response', () => {
  it('decodes a real fixture', async () => {
    const data = await produceResponseV4.decode(Buffer.from(v4ResponseFixture.data));

    expect(data).toEqual({
      topics: [
        {
          topicName: 'test-topic-5370ce2c813663fce3ca-99758-4d9ea731-5a23-4d5b-abbd-8390588d655d',
          partitions: [{ partition: 0, errorCode: 0, baseOffset: 0n, logAppendTime: -1n }],
        },
      ],
      throttleTime: 0,
    });
    await expect(produceResponseV4.parse(data)).resolves.toBeTruthy();
  });
});
