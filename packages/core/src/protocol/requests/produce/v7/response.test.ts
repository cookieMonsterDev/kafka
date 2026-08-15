import { describe, expect, it } from 'vitest';
import v7ResponseFixture from '../fixtures/v7-response.json' with { type: 'json' };
import { produceResponseV7 } from './response.js';

describe('protocol/requests/produce/v7/response', () => {
  it('decodes a real fixture', async () => {
    const data = await produceResponseV7.decode(Buffer.from(v7ResponseFixture.data));

    expect(data).toEqual({
      topics: [
        {
          topicName: 'test-topic-923030b997a626c23158-517-bdaf87ff-6ab3-4ba6-ac23-ad463d5230cd',
          partitions: [{ partition: 0, errorCode: 0, baseOffset: 0n, logAppendTime: -1n, logStartOffset: 0n }],
        },
      ],
      throttleTime: 0,
      clientSideThrottleTime: 0,
    });
    await expect(produceResponseV7.parse(data)).resolves.toBeTruthy();
  });
});
