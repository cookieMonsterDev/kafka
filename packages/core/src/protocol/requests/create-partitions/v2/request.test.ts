import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { withAssignmentDefaults } from '../v0/request';
import { createPartitionsRequestV1 } from '../v1/request';
import { createPartitionsRequestV2, requestSchema } from './request';

const payload = {
  topicPartitions: withAssignmentDefaults([
    { topic: 'orders', count: 3 },
    { topic: 'payments', count: 5, assignments: [[0], [1], [2]] },
  ]),
  timeout: 5000,
  validateOnly: false,
};

describe('protocol/requests/create-partitions/v2/request', () => {
  it('encodes compact topics and nullable compact assignments with TAG_BUFFERs', async () => {
    const definition = createPartitionsRequestV2(payload);
    expect(definition.apiVersion).toBe(2);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarInt(3)
      .writeUVarIntString('orders')
      .writeInt32(3)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarIntString('payments')
      .writeInt32(5)
      .writeUVarInt(4)
      .writeUVarInt(2)
      .writeInt32(0)
      .writeUVarInt(0)
      .writeUVarInt(2)
      .writeInt32(1)
      .writeUVarInt(0)
      .writeUVarInt(2)
      .writeInt32(2)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeInt32(5000)
      .writeBoolean(false)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the non-flexible v1 encoding', async () => {
    const v2 = await createPartitionsRequestV2(payload).encode();
    const v1 = await createPartitionsRequestV1(payload).encode();
    expect(v2.buffer).not.toEqual(v1.buffer);
  });
});
