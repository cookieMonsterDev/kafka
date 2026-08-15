import { describe, expect, it } from 'vitest';
import v5ResponseFixture from '../fixtures/v5-response.json' with { type: 'json' };
import { produceResponseV5 } from './response.js';

describe('protocol/requests/produce/v5/response', () => {
  it('decodes a real fixture, including logStartOffset', async () => {
    const data = await produceResponseV5.decode(Buffer.from(v5ResponseFixture.data));

    expect(data).toEqual({
      topics: [
        {
          topicName: 'test-topic-1c8ace0ecfb3cb281243-706-b9f24ac1-6a1e-4458-ba5f-5fc0c51a46c7',
          partitions: [{ partition: 0, errorCode: 0, baseOffset: 0n, logAppendTime: -1n, logStartOffset: 0n }],
        },
      ],
      throttleTime: 0,
    });
    await expect(produceResponseV5.parse(data)).resolves.toBeTruthy();
  });
});
