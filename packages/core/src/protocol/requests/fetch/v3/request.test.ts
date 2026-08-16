import { describe, expect, it } from 'vitest';
import v3RequestFixture from '../fixtures/v3-request.json' with { type: 'json' };
import { fetchRequestV3 } from './request';

describe('protocol/requests/fetch/v3/request', () => {
  it('encodes a request matching a captured protocol fixture', async () => {
    const encoder = await fetchRequestV3({
      replicaId: 0,
      maxWaitTime: 5,
      minBytes: 1,
      maxBytes: 10485760,
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 1048576 }] }],
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v3RequestFixture.data));
    expect(fetchRequestV3({ replicaId: 0, maxWaitTime: 5, minBytes: 1, maxBytes: 1, topics: [] }).apiVersion).toBe(3);
  });
});
