import { describe, expect, it } from 'vitest';
import { ListOffsets } from './index';

describe('protocol/requests/list-offsets', () => {
  it('implements versions 0 through 8', () => {
    expect(ListOffsets.versions).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8]);
  });

  it('defaults currentLeaderEpoch to -1 on v4+ and does not write it on v3', async () => {
    const topics = [{ topic: 't', partitions: [{ partition: 0, timestamp: 1n, currentLeaderEpoch: 9 }] }];
    const v3 = await ListOffsets.protocol({ version: 3 })({
      replicaId: -1,
      isolationLevel: 0,
      topics,
    }).request.encode();
    const v4 = await ListOffsets.protocol({ version: 4 })({
      replicaId: -1,
      isolationLevel: 0,
      topics,
    }).request.encode();
    expect(v4.buffer.length).toBe(v3.buffer.length + 4);

    const omitted = await ListOffsets.protocol({ version: 4 })({
      replicaId: -1,
      isolationLevel: 0,
      topics: [{ topic: 't', partitions: [{ partition: 0, timestamp: 1n }] }],
    }).request.encode();
    const explicitUnknown = await ListOffsets.protocol({ version: 4 })({
      replicaId: -1,
      isolationLevel: 0,
      topics: [{ topic: 't', partitions: [{ partition: 0, timestamp: 1n, currentLeaderEpoch: -1 }] }],
    }).request.encode();
    expect(omitted.buffer).toEqual(explicitUnknown.buffer);
  });
});
