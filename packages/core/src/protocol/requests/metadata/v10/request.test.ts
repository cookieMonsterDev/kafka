import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { ZERO_TOPIC_ID } from '../shared';
import { metadataRequestV9 } from '../v9/request';
import { metadataRequestV10 } from './request';

describe('protocol/requests/metadata/v10/request', () => {
  it('encodes name-only topics as { zero topicId, name } structs', async () => {
    const payload = { topics: ['orders'], allowAutoTopicCreation: true };
    const definition = metadataRequestV10(payload);
    expect(definition.apiVersion).toBe(10);

    const encoder = await definition.encode();
    const v9 = await metadataRequestV9(payload).encode();
    expect(encoder.buffer).not.toEqual(v9.buffer);

    const expected = new Encoder()
      .writeUVarInt(2)
      .writeBuffer(ZERO_TOPIC_ID)
      .writeUVarIntString('orders')
      .writeUVarInt(0)
      .writeBoolean(true)
      .writeBoolean(false)
      .writeBoolean(false)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);

    const decoder = new Decoder(encoder.buffer);
    expect(decoder.readUVarInt() - 1).toBe(1);
    expect(decoder.readBytes(16)).toEqual(ZERO_TOPIC_ID);
    expect(decoder.readUVarIntString()).toBe('orders');
    expect(decoder.readUVarInt()).toBe(0);
    expect(decoder.readBoolean()).toBe(true);
    expect(decoder.readBoolean()).toBe(false);
    expect(decoder.readBoolean()).toBe(false);
    expect(decoder.readUVarInt()).toBe(0);
  });

  it('encodes empty topics as a compact null (all topics)', async () => {
    const encoder = await metadataRequestV10({ topics: [], allowAutoTopicCreation: true }).encode();
    expect(encoder.buffer).toEqual(Buffer.from([0, 1, 0, 0, 0]));
  });
});
