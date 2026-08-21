import { describe, expect, it } from 'vitest';
import { Fetch } from './index';
import type { FetchRequestOptions } from './shared';

const topics: FetchRequestOptions['topics'] = [
  {
    topic: 'test-topic',
    partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 1_048_576 }],
  },
];

function fetchOptions(maxWaitTime: number): FetchRequestOptions {
  return {
    replicaId: -1,
    maxWaitTime,
    minBytes: 1,
    maxBytes: 1024,
    topics,
  };
}

describe('protocol/requests/fetch', () => {
  it('implements versions 0 through 18 (MessageSet v0-v3, RecordBatch v4-v18)', () => {
    expect(Fetch.versions).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]);
  });

  for (const version of Fetch.versions) {
    describe(`v${version}`, () => {
      it('exposes requestTimeout as maxWaitTime plus network delay', () => {
        const maxWaitTime = 1000;
        const protocol = Fetch.protocol({ version })(fetchOptions(maxWaitTime));
        expect(protocol.requestTimeout).toBe(maxWaitTime + 100);
      });

      it('does not overflow MAX_SAFE_INTEGER when adding the network delay', () => {
        const protocol = Fetch.protocol({ version })(fetchOptions(Number.MAX_SAFE_INTEGER));
        expect(protocol.requestTimeout).toBe(Number.MAX_SAFE_INTEGER);
      });
    });
  }
});
