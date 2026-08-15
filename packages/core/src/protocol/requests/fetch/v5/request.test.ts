import { describe, expect, it } from 'vitest';
import v5RequestFixture from '../fixtures/v5-request.json' with { type: 'json' };
import { fetchRequestV5 } from './request.js';

describe('protocol/requests/fetch/v5/request', () => {
  it('encodes a request matching a real kafkajs fixture', async () => {
    const encoder = await fetchRequestV5({
      replicaId: -1,
      maxWaitTime: 100,
      minBytes: 1,
      maxBytes: 10485760,
      topics: [
        {
          topic: 'test-topic-c935d678835de2c9c79e-2064-677041b7-df54-4d4d-a53a-b9133d2fdc8c',
          partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 1048576 }],
        },
      ],
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v5RequestFixture.data));
  });
});
