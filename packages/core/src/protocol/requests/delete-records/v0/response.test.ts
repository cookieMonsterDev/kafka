import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { deleteRecordsResponseV0 } from './response';

describe('protocol/requests/delete-records/v0/response', () => {
  it('decodes a real fixture', async () => {
    const response = deleteRecordsResponseV0({ topics: [] });
    const data = await response.decode(Buffer.from(v0ResponseFixture.data));

    expect(data).toEqual({
      throttleTime: 0,
      topics: [
        {
          topic: 'test-topic-5da683fa3b1898223498-97119-d06829e3-35d2-4b97-b4b4-7c03d4ad7cc8',
          partitions: [{ partition: 0, lowWatermark: 7n, errorCode: 0 }],
        },
      ],
    });
    await expect(response.parse(data)).resolves.toBeTruthy();
  });
});
