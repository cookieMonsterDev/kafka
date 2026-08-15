import { describe, expect, it } from 'vitest';
import v2ResponseFixture from '../fixtures/v2-response.json' with { type: 'json' };
import { offsetCommitResponseV2 } from './response.js';

describe('protocol/requests/offset-commit/v2/response', () => {
  it('decodes a real fixture', async () => {
    const data = await offsetCommitResponseV2.decode(Buffer.from(v2ResponseFixture.data));
    expect(data).toEqual({
      responses: [{ topic: 'test-topic-eb1a285cda2e9f9a1021', partitions: [{ partition: 0, errorCode: 0 }] }],
    });
    await expect(offsetCommitResponseV2.parse(data)).resolves.toBe(data);
  });

  it('throws on the first partition-level failure', async () => {
    const data = {
      responses: [{ topic: 't', partitions: [{ partition: 0, errorCode: 27 }] }],
    };
    await expect(offsetCommitResponseV2.parse(data)).rejects.toThrow();
  });
});
