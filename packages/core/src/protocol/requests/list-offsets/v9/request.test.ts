import { describe, expect, it } from 'vitest';
import { listOffsetsRequestV8 } from '../v8/request';
import { listOffsetsRequestV9 } from './request';

const payload = {
  replicaId: -1,
  isolationLevel: 1,
  topics: [
    {
      topic: 'payments',
      partitions: [{ partition: 2, currentLeaderEpoch: -1, timestamp: -5n }],
    },
  ],
};

describe('protocol/requests/list-offsets/v9/request', () => {
  it('uses apiVersion 9 with the same compact body as v8', async () => {
    const definition = listOffsetsRequestV9(payload);
    expect(definition.apiVersion).toBe(9);

    const [v9, v8] = await Promise.all([definition.encode(), listOffsetsRequestV8(payload).encode()]);
    expect(v9.buffer).toEqual(v8.buffer);
  });
});
