import { describe, expect, it } from 'vitest';
import { listOffsetsRequestV10 } from '../v10/request';
import { listOffsetsRequestV11 } from './request';

const payload = {
  replicaId: -1,
  isolationLevel: 1,
  topics: [{ topic: 'payments', partitions: [{ partition: 2, currentLeaderEpoch: -1, timestamp: -6n }] }],
  timeoutMs: 30_000,
};

describe('protocol/requests/list-offsets/v11/request', () => {
  it('uses apiVersion 11 with the same compact body as v10', async () => {
    const definition = listOffsetsRequestV11(payload);
    expect(definition.apiVersion).toBe(11);

    const [v11, v10] = await Promise.all([definition.encode(), listOffsetsRequestV10(payload).encode()]);
    expect(v11.buffer).toEqual(v10.buffer);
  });
});
