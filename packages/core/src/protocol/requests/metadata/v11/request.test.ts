import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { ZERO_TOPIC_ID } from '../shared';
import { metadataRequestV10 } from '../v10/request';
import { metadataRequestV11 } from './request';

describe('protocol/requests/metadata/v11/request', () => {
  it('omits includeClusterAuthorizedOperations', async () => {
    const payload = { topics: ['orders'], allowAutoTopicCreation: true };
    const definition = metadataRequestV11(payload);
    expect(definition.apiVersion).toBe(11);

    const encoder = await definition.encode();
    const v10 = await metadataRequestV10(payload).encode();
    expect(encoder.buffer).not.toEqual(v10.buffer);

    const expected = new Encoder()
      .writeUVarInt(2)
      .writeBuffer(ZERO_TOPIC_ID)
      .writeUVarIntString('orders')
      .writeUVarInt(0)
      .writeBoolean(true)
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
    expect(decoder.readUVarInt()).toBe(0);
  });

  it('encodes empty topics as a compact null without the cluster include flag', async () => {
    const encoder = await metadataRequestV11({ topics: [], allowAutoTopicCreation: true }).encode();
    expect(encoder.buffer).toEqual(Buffer.from([0, 1, 0, 0]));
  });
});
