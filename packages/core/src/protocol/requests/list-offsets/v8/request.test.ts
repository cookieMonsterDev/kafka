import { describe, expect, it } from 'vitest';
import { listOffsetsRequestV6 } from '../v6/request';
import { listOffsetsRequestV7 } from '../v7/request';
import { listOffsetsRequestV8 } from './request';

const payload = {
  replicaId: -1,
  isolationLevel: 1,
  topics: [
    {
      topic: 'payments',
      partitions: [{ partition: 2, currentLeaderEpoch: -1, timestamp: -1n }],
    },
  ],
};

describe('protocol/requests/list-offsets/v8/request', () => {
  it('uses apiVersion 8 with the same compact body as v6 and v7', async () => {
    const definition = listOffsetsRequestV8(payload);
    expect(definition.apiVersion).toBe(8);

    const [v8, v7, v6] = await Promise.all([
      definition.encode(),
      listOffsetsRequestV7(payload).encode(),
      listOffsetsRequestV6(payload).encode(),
    ]);
    expect(v8.buffer).toEqual(v7.buffer);
    expect(v8.buffer).toEqual(v6.buffer);
  });
});
