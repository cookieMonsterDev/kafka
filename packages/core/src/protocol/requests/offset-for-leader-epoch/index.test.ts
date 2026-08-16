import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { Encoder } from '../../encoder';
import { OffsetForLeaderEpoch } from './index';
import { offsetForLeaderEpochRequestV0 } from './v0/request';
import { offsetForLeaderEpochResponseV0 } from './v0/response';
import { offsetForLeaderEpochRequestV2 } from './v2/request';
import { offsetForLeaderEpochRequestV3 } from './v3/request';
import { offsetForLeaderEpochResponseV3 } from './v3/response';

const sampleTopics = [{ topic: 'orders', partitions: [{ partition: 0, currentLeaderEpoch: 3, leaderEpoch: 5 }] }];

describe('protocol/requests/offset-for-leader-epoch', () => {
  it('implements versions 0 through 4', () => {
    expect(OffsetForLeaderEpoch.versions).toEqual([0, 1, 2, 3, 4]);
  });

  it('round-trips a v0 request encode and response decode', async () => {
    const { request, response } = OffsetForLeaderEpoch.protocol({ version: 0 })({
      replicaId: 99,
      topics: sampleTopics,
    });

    expect(request.apiVersion).toBe(0);
    const encoded = await request.encode();
    const expectedRequest = new Encoder().writeArray([
      new Encoder().writeString('orders').writeArray([new Encoder().writeInt32(0).writeInt32(5)]),
    ]);
    expect(encoded.buffer).toEqual(expectedRequest.buffer);

    const decoder = new Decoder(encoded.buffer);
    expect(decoder.readInt32()).toBe(1);
    expect(decoder.readString()).toBe('orders');
    expect(decoder.readInt32()).toBe(1);
    expect(decoder.readInt32()).toBe(0);
    expect(decoder.readInt32()).toBe(5);
    expect(decoder.offset).toBe(encoded.buffer.length);

    const responseBuffer = new Encoder().writeArray([
      new Encoder().writeString('orders').writeArray([new Encoder().writeInt16(0).writeInt32(0).writeInt64(42n)]),
    ]).buffer;
    const decoded = await response.decode(responseBuffer);
    expect(decoded).toEqual({
      topics: [{ topic: 'orders', partitions: [{ errorCode: 0, partition: 0, endOffset: 42n }] }],
    });
    await expect(response.parse(decoded)).resolves.toEqual(decoded);
  });

  it('round-trips a v2 request encode and response decode, including throttleTime', async () => {
    const { request, response } = OffsetForLeaderEpoch.protocol({ version: 2 })({
      topics: [{ topic: 'orders', partitions: [{ partition: 0, leaderEpoch: 5 }] }],
    });

    expect(request.apiVersion).toBe(2);
    const encoded = await request.encode();
    const expectedRequest = new Encoder()
      .writeInt32(-1)
      .writeArray([
        new Encoder().writeString('orders').writeArray([new Encoder().writeInt32(0).writeInt32(-1).writeInt32(5)]),
      ]);
    expect(encoded.buffer).toEqual(expectedRequest.buffer);

    const decoder = new Decoder(encoded.buffer);
    expect(decoder.readInt32()).toBe(-1);
    expect(decoder.readInt32()).toBe(1);
    expect(decoder.readString()).toBe('orders');
    expect(decoder.readInt32()).toBe(1);
    expect(decoder.readInt32()).toBe(0);
    expect(decoder.readInt32()).toBe(-1);
    expect(decoder.readInt32()).toBe(5);
    expect(decoder.offset).toBe(encoded.buffer.length);

    const responseBuffer = new Encoder()
      .writeInt32(17)
      .writeArray([
        new Encoder()
          .writeString('orders')
          .writeArray([new Encoder().writeInt16(0).writeInt32(0).writeInt32(5).writeInt64(42n)]),
      ]).buffer;
    const decoded = await response.decode(responseBuffer);
    expect(decoded).toEqual({
      throttleTime: 17,
      topics: [{ topic: 'orders', partitions: [{ errorCode: 0, partition: 0, leaderEpoch: 5, endOffset: 42n }] }],
    });
    await expect(response.parse(decoded)).resolves.toEqual(decoded);
  });

  it('encodes v3 topic names with a compact (uvarint) length prefix, not int16', async () => {
    const definition = offsetForLeaderEpochRequestV3({
      replicaId: -1,
      topics: [{ topic: 'orders', partitions: [{ partition: 0, currentLeaderEpoch: -1, leaderEpoch: 5 }] }],
    });
    const encoded = await definition.encode();

    const expected = new Encoder()
      .writeInt32(-1)
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeUVarInt(2)
      .writeInt32(0)
      .writeInt32(-1)
      .writeInt32(5)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarInt(0);
    expect(encoded.buffer).toEqual(expected.buffer);

    // Classic STRING is int16 length (00 06 for "orders"); compact is uvarint(length+1)=07.
    expect(encoded.buffer[4]).toBe(2);
    expect(encoded.buffer[5]).toBe('orders'.length + 1);

    const decoder = new Decoder(encoded.buffer);
    expect(decoder.readInt32()).toBe(-1);
    expect(decoder.readUVarInt()).toBe(2);
    expect(decoder.readUVarIntString()).toBe('orders');
  });

  it('throws the first partition failure with topic and partition extras', async () => {
    const encoded = new Encoder().writeArray([
      new Encoder()
        .writeString('orders')
        .writeArray([
          new Encoder().writeInt16(0).writeInt32(0).writeInt64(1n),
          new Encoder().writeInt16(74).writeInt32(7).writeInt64(-1n),
        ]),
    ]);
    const data = await offsetForLeaderEpochResponseV0.decode(encoded.buffer);
    await expect(offsetForLeaderEpochResponseV0.parse(data)).rejects.toMatchObject({
      type: 'FENCED_LEADER_EPOCH',
      topic: 'orders',
      partition: 7,
      message: expect.stringContaining('topic: orders, partition: 7'),
    });
  });

  it('v0 request factory matches the family encoder', async () => {
    const fromFamily = await OffsetForLeaderEpoch.protocol({ version: 0 })({ topics: sampleTopics }).request.encode();
    const fromFactory = await offsetForLeaderEpochRequestV0({
      topics: [{ topic: 'orders', partitions: [{ partition: 0, leaderEpoch: 5 }] }],
    }).encode();
    expect(fromFamily.buffer).toEqual(fromFactory.buffer);
  });

  it('v2 request factory matches the family encoder', async () => {
    const fromFamily = await OffsetForLeaderEpoch.protocol({ version: 2 })({
      replicaId: -1,
      topics: sampleTopics,
    }).request.encode();
    const fromFactory = await offsetForLeaderEpochRequestV2({
      replicaId: -1,
      topics: [{ topic: 'orders', partitions: [{ partition: 0, currentLeaderEpoch: 3, leaderEpoch: 5 }] }],
    }).encode();
    expect(fromFamily.buffer).toEqual(fromFactory.buffer);
  });

  it('decodes a flexible v3 response', async () => {
    const encoded = new Encoder()
      .writeInt32(0)
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeUVarInt(2)
      .writeInt16(0)
      .writeInt32(0)
      .writeInt32(5)
      .writeInt64(42n)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarInt(0);
    const decoded = await offsetForLeaderEpochResponseV3.decode(encoded.buffer);
    expect(decoded).toEqual({
      throttleTime: 0,
      topics: [{ topic: 'orders', partitions: [{ errorCode: 0, partition: 0, leaderEpoch: 5, endOffset: 42n }] }],
    });
  });
});
