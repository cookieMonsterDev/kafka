import { describe, expect, it } from 'vitest';
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' };
import { listOffsetsResponseV1 } from './response';

describe('protocol/requests/list-offsets/v1/response', () => {
  it('decodes a real fixture, offsets and timestamps as bigint', async () => {
    const data = await listOffsetsResponseV1.decode(Buffer.from(v1ResponseFixture.data));

    expect(data).toEqual({
      responses: [
        {
          topic: 'test-topic-16e956902e39874d06f5-91705-2958a472-e582-47a4-86f0-b258630fb3e6',
          partitions: [{ partition: 0, errorCode: 0, timestamp: 1543343103774n, offset: 0n }],
        },
      ],
    });
    await expect(listOffsetsResponseV1.parse(data)).resolves.toBe(data);
  });

  it('throws on the first partition-level failure', async () => {
    const data = {
      responses: [
        { topic: 't', partitions: [{ partition: 0, errorCode: 0, timestamp: -1n, offset: 0n }] },
        { topic: 't2', partitions: [{ partition: 0, errorCode: 3, timestamp: -1n, offset: -1n }] },
      ],
    };
    await expect(listOffsetsResponseV1.parse(data)).rejects.toThrow(/does not host this topic-partition/);
  });
});
