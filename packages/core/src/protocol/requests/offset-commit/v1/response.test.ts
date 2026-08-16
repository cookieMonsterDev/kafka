import { describe, expect, it } from 'vitest';
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' };
import { offsetCommitResponseV1 } from './response';

describe('protocol/requests/offset-commit/v1/response', () => {
  it('decodes a real fixture', async () => {
    const data = await offsetCommitResponseV1.decode(Buffer.from(v1ResponseFixture.data));
    expect(data).toEqual({
      responses: [{ topic: 'test-topic-eb1a285cda2e9f9a1021', partitions: [{ partition: 0, errorCode: 0 }] }],
    });
    await expect(offsetCommitResponseV1.parse(data)).resolves.toBe(data);
  });
});
