import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { listOffsetsRequestV4 } from '../v4/request';
import { listOffsetsRequestV6 } from './request';

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

describe('protocol/requests/list-offsets/v6/request', () => {
  it('encodes compact strings/arrays and a TAG_BUFFER on every struct', async () => {
    const definition = listOffsetsRequestV6(payload);
    expect(definition.apiVersion).toBe(6);

    const encoder = await definition.encode();
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

    expect(encoder.buffer).toEqual(expected.buffer);
  });

  it('is not the non-flexible v4 encoding', async () => {
    const v6 = await listOffsetsRequestV6(payload).encode();
    const v4 = await listOffsetsRequestV4(payload).encode();
    expect(v6.buffer).not.toEqual(v4.buffer);
  });
});
