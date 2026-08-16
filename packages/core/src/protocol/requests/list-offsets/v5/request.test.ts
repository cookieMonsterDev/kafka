import { describe, expect, it } from 'vitest';
import { listOffsetsRequestV4 } from '../v4/request';
import { listOffsetsRequestV5 } from './request';

const payload = {
  replicaId: -1,
  isolationLevel: 1,
  topics: [
    {
      topic: 'orders',
      partitions: [{ partition: 2, currentLeaderEpoch: 7, timestamp: 42n }],
    },
  ],
};

describe('protocol/requests/list-offsets/v5/request', () => {
  it('encodes identically to v4, wire-for-wire', async () => {
    const definition = listOffsetsRequestV5(payload);
    expect(definition.apiVersion).toBe(5);
    const encoder = await definition.encode();
    const v4 = await listOffsetsRequestV4(payload).encode();
    expect(encoder.buffer).toEqual(v4.buffer);
  });
});
