import { describe, expect, it } from 'vitest';
import v7RequestFixture from '../fixtures/v7-request.json' with { type: 'json' };
import { fetchRequestV7 } from './request.js';

describe('protocol/requests/fetch/v7/request', () => {
  it('encodes a request matching a real kafkajs fixture', async () => {
    const encoder = await fetchRequestV7({
      replicaId: -1,
      maxWaitTime: 100,
      minBytes: 1,
      maxBytes: 10485760,
      topics: [
        {
          topic: 'test-topic-2077b9d2b36c4082e594-4020-b5a52b27-56df-4b87-800d-82c1cf26317d',
          partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 1048576 }],
        },
      ],
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v7RequestFixture.data));
  });
});
