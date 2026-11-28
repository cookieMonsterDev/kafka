import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { listOffsetsRequestV10 } from './request';

describe('protocol/requests/list-offsets/v10/request', () => {
  it('writes timeoutMs after the topics', async () => {
    const definition = listOffsetsRequestV10({
      replicaId: -1,
      isolationLevel: 0,
      topics: [{ topic: 'orders', partitions: [{ partition: 0, currentLeaderEpoch: 3, timestamp: -5n }] }],
      timeoutMs: 15_000,
    });
    expect(definition.apiVersion).toBe(10);

    const expected = new Encoder()
      .writeInt32(-1)
      .writeInt8(0)
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeUVarInt(2)
      .writeInt32(0)
      .writeInt32(3)
      .writeInt64(-5n)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeInt32(15_000)
      .writeUVarInt(0);
    expect((await definition.encode()).buffer).toEqual(expected.buffer);
  });
});
