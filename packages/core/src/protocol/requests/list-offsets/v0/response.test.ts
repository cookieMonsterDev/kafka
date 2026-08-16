import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { listOffsetsResponseV0 } from './response';

describe('protocol/requests/list-offsets/v0/response', () => {
  it('decodes a real fixture, offsets as bigint[]', async () => {
    const data = await listOffsetsResponseV0.decode(Buffer.from(v0ResponseFixture.data));

    expect(data).toEqual({
      responses: [
        {
          topic: 'test-topic-727705ce68c29fedddf4',
          partitions: [{ partition: 0, errorCode: 0, offsets: [0n] }],
        },
      ],
    });
    await expect(listOffsetsResponseV0.parse(data)).resolves.toBe(data);
  });
});
