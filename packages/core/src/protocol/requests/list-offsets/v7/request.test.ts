import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { listOffsetsRequestV6 } from '../v6/request';
import { listOffsetsRequestV7 } from './request';

const payload = {
  replicaId: -1,
  isolationLevel: 0,
  topics: [
    {
      topic: 'orders',
      partitions: [{ partition: 0, currentLeaderEpoch: 3, timestamp: 1509285569484n }],
    },
  ],
};

describe('protocol/requests/list-offsets/v7/request', () => {
  it('uses apiVersion 7 with the same compact body as v6', async () => {
    const definition = listOffsetsRequestV7(payload);
    expect(definition.apiVersion).toBe(7);

    const v7 = await definition.encode();
    const v6 = await listOffsetsRequestV6(payload).encode();
    expect(v7.buffer).toEqual(v6.buffer);

    const expected = new Encoder()
      .writeInt32(-1)
      .writeInt8(0)
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeUVarInt(2)
      .writeInt32(0)
      .writeInt32(3)
      .writeInt64(1509285569484n)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarInt(0);
    expect(v7.buffer).toEqual(expected.buffer);
  });
});
