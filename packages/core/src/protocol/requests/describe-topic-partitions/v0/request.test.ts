import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { DEFAULT_RESPONSE_PARTITION_LIMIT, describeTopicPartitionsRequestV0, requestSchema } from './request';

describe('protocol/requests/describe-topic-partitions/v0/request', () => {
  it('encodes a name-only request with a null cursor and the default partition limit', async () => {
    const encoder = await describeTopicPartitionsRequestV0({ topics: [{ topic: 'orders' }] }).encode();
    const expected = new Encoder()
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeUVarInt(0)
      .writeInt32(DEFAULT_RESPONSE_PARTITION_LIMIT)
      .writeInt8(-1)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual({
      topics: [{ topic: 'orders' }],
      responsePartitionLimit: DEFAULT_RESPONSE_PARTITION_LIMIT,
      cursor: null,
    });
  });

  it('encodes a cursor and a custom response partition limit', async () => {
    const value = {
      topics: [{ topic: 'orders' }, { topic: 'payments' }],
      responsePartitionLimit: 1,
      cursor: { topic: 'orders', partitionIndex: 3 },
    };
    const encoder = await describeTopicPartitionsRequestV0(value).encode();
    const expected = new Encoder()
      .writeUVarInt(3)
      .writeUVarIntString('orders')
      .writeUVarInt(0)
      .writeUVarIntString('payments')
      .writeUVarInt(0)
      .writeInt32(1)
      .writeInt8(1)
      .writeUVarIntString('orders')
      .writeInt32(3)
      .writeUVarInt(0)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
