import { describe, expect, it } from 'vitest';
import { offsetCommitRequestV8 } from '../v8/request';
import { offsetCommitRequestV9 } from './request';

const payload = {
  groupId: 'g1',
  groupGenerationId: 4,
  memberId: 'm1',
  groupInstanceId: null as string | null,
  topics: [
    {
      topic: 'orders',
      partitions: [{ partition: 0, offset: 42n, leaderEpoch: 3, metadata: null as string | null }],
    },
  ],
};

describe('protocol/requests/offset-commit/v9/request', () => {
  it('is the same encoding as version 8 with apiVersion 9', async () => {
    const definition = offsetCommitRequestV9(payload);
    expect(definition.apiVersion).toBe(9);
    const v9 = await definition.encode();
    const v8 = await offsetCommitRequestV8(payload).encode();
    expect(v9.buffer).toEqual(v8.buffer);
  });
});
