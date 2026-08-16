import { describe, expect, it } from 'vitest';
import v2RequestFixture from '../fixtures/v2-request.json' with { type: 'json' };
import { fetchRequestV2 } from './request';

describe('protocol/requests/fetch/v2/request', () => {
  it('encodes a request matching a captured protocol fixture', async () => {
    const encoder = await fetchRequestV2({
      replicaId: 0,
      maxWaitTime: 5,
      minBytes: 1,
      maxBytes: 1048576,
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 1048576 }] }],
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v2RequestFixture.data));
    expect(fetchRequestV2({ replicaId: 0, maxWaitTime: 5, minBytes: 1, maxBytes: 1, topics: [] }).apiVersion).toBe(2);
  });
});
