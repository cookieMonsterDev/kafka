import { describe, expect, it } from 'vitest';
import v6ResponseFixture from '../fixtures/v6-response.json' with { type: 'json' };
import { produceResponseV6 } from './response.js';

describe('protocol/requests/produce/v6/response', () => {
  it('decodes a real fixture, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await produceResponseV6.decode(Buffer.from(v6ResponseFixture.data));

    expect(data).toEqual({
      topics: [
        {
          topicName: 'test-topic-390850453b1c004039ea-1417-1c32a507-edbb-481d-9d9c-e287743f4b74',
          partitions: [{ partition: 0, errorCode: 0, baseOffset: 0n, logAppendTime: -1n, logStartOffset: 0n }],
        },
      ],
      throttleTime: 0,
      clientSideThrottleTime: 0,
    });
    await expect(produceResponseV6.parse(data)).resolves.toBeTruthy();
  });
});
