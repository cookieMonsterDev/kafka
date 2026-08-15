import { describe, expect, it } from 'vitest';
import v4RequestFixture from '../fixtures/v4-request.json' with { type: 'json' };
import { fetchRequestV4 } from './request.js';

describe('protocol/requests/fetch/v4/request', () => {
  it('encodes a request matching a real kafkajs fixture', async () => {
    // kafkajs's own captured fixture omitted `replicaId`, which its untyped JS silently
    // coerced to 0 on the wire; this port requires it explicitly.
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
