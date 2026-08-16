import { describe, expect, it } from 'vitest';
import v6RequestFixture from '../fixtures/v6-request.json' with { type: 'json' };
import { fetchRequestV6 } from './request';

describe('protocol/requests/fetch/v6/request', () => {
  it('encodes a request matching a captured protocol fixture', async () => {
    const encoder = await fetchRequestV6({
      replicaId: -1,
      maxWaitTime: 100,
      minBytes: 1,
      maxBytes: 10485760,
      topics: [
        {
          topic: 'test-topic-07eae0edd6400fe2733a-3088-330080bb-97f1-4a09-89e1-f0fe5c137ab2',
          partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 1048576 }],
        },
      ],
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v6RequestFixture.data));
  });
});
