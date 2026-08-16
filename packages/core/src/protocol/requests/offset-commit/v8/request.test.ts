import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { offsetCommitRequestV7 } from '../v7/request';
import { offsetCommitRequestV8, requestSchema } from './request';

const payload = {
  groupId: 'g1',
  groupGenerationId: 1,
  memberId: 'm1',
  groupInstanceId: 'instance-1' as string | null,
  topics: [
    {
      topic: 'orders',
      partitions: [{ partition: 0, offset: 42n, leaderEpoch: 3, metadata: 'meta' }],
    },
  ],
};

describe('protocol/requests/offset-commit/v8/request', () => {
  it('encodes compact strings/arrays and a TAG_BUFFER on every struct', async () => {
    const definition = offsetCommitRequestV8(payload);
    expect(definition.apiVersion).toBe(8);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarIntString('g1')
      .writeInt32(1)
      .writeUVarIntString('m1')
      .writeUVarIntString('instance-1')
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeUVarInt(2)
      .writeInt32(0)
      .writeInt64(42n)
      .writeInt32(3)
      .writeUVarIntString('meta')
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('encodes a null groupInstanceId as compact null', async () => {
    const encoder = await offsetCommitRequestV8({ ...payload, groupInstanceId: null }).encode();
    const expected = new Encoder()
      .writeUVarIntString('g1')
      .writeInt32(1)
      .writeUVarIntString('m1')
      .writeUVarIntString(null)
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeUVarInt(2)
      .writeInt32(0)
      .writeInt64(42n)
      .writeInt32(3)
      .writeUVarIntString('meta')
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
  });

  it('is not the non-flexible v7 encoding', async () => {
    const v8 = await offsetCommitRequestV8(payload).encode();
    const v7 = await offsetCommitRequestV7(payload).encode();
    expect(v8.buffer).not.toEqual(v7.buffer);
  });
});
