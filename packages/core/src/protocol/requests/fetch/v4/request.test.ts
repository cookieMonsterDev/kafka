import { describe, expect, it } from 'vitest';
import v4RequestFixture from '../fixtures/v4-request.json' with { type: 'json' };
import { fetchRequestV4 } from './request';

describe('protocol/requests/fetch/v4/request', () => {
  it('encodes a request matching a captured protocol fixture', async () => {
    // The captured fixture omitted `replicaId`; the encoder requires it explicitly (0 on the wire).
    const encoder = await fetchRequestV4({
      replicaId: 0,
      maxWaitTime: 5,
      minBytes: 1,
      maxBytes: 10485760,
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 1048576 }] }],
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v4RequestFixture.data));
  });
});
