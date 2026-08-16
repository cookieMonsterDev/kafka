import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { createTopicsRequestV4 } from '../v4/request';
import { withTopicDefaults } from '../v2/request';
import { createTopicsRequestV5, requestSchema } from './request';

const payload = {
  topics: withTopicDefaults([
    {
      topic: 'orders',
      replicaAssignment: [{ partition: 0, replicas: [1, 2] }],
      configEntries: [{ name: 'cleanup.policy', value: 'compact' }],
    },
  ]),
  timeout: 5000,
  validateOnly: false,
};

describe('protocol/requests/create-topics/v5/request', () => {
  it('encodes compact strings/arrays and a TAG_BUFFER on every struct', async () => {
    const definition = createTopicsRequestV5(payload);
    expect(definition.apiVersion).toBe(5);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeInt32(-1)
      .writeInt16(-1)
      .writeUVarInt(2)
      .writeInt32(0)
      .writeUVarInt(3)
      .writeInt32(1)
      .writeInt32(2)
      .writeUVarInt(0)
      .writeUVarInt(2)
      .writeUVarIntString('cleanup.policy')
      .writeUVarIntString('compact')
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeInt32(5000)
      .writeBoolean(false)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('encodes empty assignments and configs as compact empty arrays', async () => {
    const encoder = await createTopicsRequestV5({
      topics: withTopicDefaults([{ topic: 'orders' }]),
      timeout: 5000,
      validateOnly: true,
    }).encode();

    const expected = new Encoder()
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeInt32(-1)
      .writeInt16(-1)
      .writeUVarInt(1)
      .writeUVarInt(1)
      .writeUVarInt(0)
      .writeInt32(5000)
      .writeBoolean(true)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
  });

  it('is not the non-flexible v4 encoding', async () => {
    const v5 = await createTopicsRequestV5(payload).encode();
    const v4 = await createTopicsRequestV4(payload).encode();
    expect(v5.buffer).not.toEqual(v4.buffer);
  });
});
