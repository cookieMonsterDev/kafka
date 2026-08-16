import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { deleteTopicsRequestV0 } from './request';

describe('protocol/requests/delete-topics/v0/request', () => {
  it('encodes a real fixture', async () => {
    const encoder = await deleteTopicsRequestV0({
      topics: ['test-topic-5f80283ca8a1e46d2273', 'test-topic-34631544b8db1d1263b9'],
      timeout: 5000,
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
