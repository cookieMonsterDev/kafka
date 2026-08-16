import { describe, expect, it } from 'vitest';
import v3ResponseFixture from '../fixtures/v3-response.json' with { type: 'json' };
import { offsetCommitResponseV3 } from './response';

describe('protocol/requests/offset-commit/v3/response', () => {
  it('decodes a real fixture, including throttleTime', async () => {
    const data = await offsetCommitResponseV3.decode(Buffer.from(v3ResponseFixture.data));
    expect(data).toEqual({
      throttleTime: 0,
      responses: [
        {
          topic: 'test-topic-5c24efe0ac41b91bee85-9985-841d6145-c897-4471-bd09-acd8b4c905f2',
          partitions: [{ partition: 0, errorCode: 0 }],
        },
      ],
    });
    await expect(offsetCommitResponseV3.parse(data)).resolves.toBe(data);
  });
});
