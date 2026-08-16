import { describe, expect, it } from 'vitest';
import v1RequestFixture from '../fixtures/v1-request.json' with { type: 'json' };
import { fetchRequestV1 } from './request';

describe('protocol/requests/fetch/v1/request', () => {
  it('encodes a request matching a captured protocol fixture', async () => {
    const encoder = await fetchRequestV1({
      replicaId: 0,
      maxWaitTime: 5,
      minBytes: 1,
      maxBytes: 1048576,
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 1048576 }] }],
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v1RequestFixture.data));
    expect(fetchRequestV1({ replicaId: 0, maxWaitTime: 5, minBytes: 1, maxBytes: 1, topics: [] }).apiVersion).toBe(1);
  });
});
