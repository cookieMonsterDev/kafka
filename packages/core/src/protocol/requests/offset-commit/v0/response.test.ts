import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { offsetCommitResponseV0 } from './response';

describe('protocol/requests/offset-commit/v0/response', () => {
  it('decodes a real fixture', async () => {
    const data = await offsetCommitResponseV0.decode(Buffer.from(v0ResponseFixture.data));
    expect(data).toEqual({
      responses: [{ topic: 'test-topic-eb1a285cda2e9f9a1021', partitions: [{ partition: 0, errorCode: 0 }] }],
    });
    await expect(offsetCommitResponseV0.parse(data)).resolves.toBe(data);
  });
});
