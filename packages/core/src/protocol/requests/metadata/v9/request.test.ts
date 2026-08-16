import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { metadataRequestV6 } from '../v6/request';
import { metadataRequestV9 } from './request';

describe('protocol/requests/metadata/v9/request', () => {
  it('encodes compact nullable topics rather than INT32 length prefixes', async () => {
    const payload = { topics: ['orders'], allowAutoTopicCreation: true };
    const definition = metadataRequestV9(payload);
    expect(definition.apiVersion).toBe(9);

    const encoder = await definition.encode();
    const v6 = await metadataRequestV6(payload).encode();
    expect(encoder.buffer).not.toEqual(v6.buffer);
    // Compact array of 1 topic is uvarint(2), not INT32 1 (0x00 0x00 0x00 0x01).
    expect(encoder.buffer[0]).toBe(2);

    const expected = new Encoder()
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeUVarInt(0)
      .writeBoolean(true)
      .writeBoolean(false)
      .writeBoolean(false)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);

    const decoder = new Decoder(encoder.buffer);
    expect(decoder.readUVarInt() - 1).toBe(1);
    expect(decoder.readUVarIntString()).toBe('orders');
    expect(decoder.readUVarInt()).toBe(0);
    expect(decoder.readBoolean()).toBe(true);
    expect(decoder.readBoolean()).toBe(false);
    expect(decoder.readBoolean()).toBe(false);
    expect(decoder.readUVarInt()).toBe(0);
  });

  it('encodes empty topics as a compact null (all topics)', async () => {
    const encoder = await metadataRequestV9({ topics: [], allowAutoTopicCreation: true }).encode();
    expect(encoder.buffer).toEqual(Buffer.from([0, 1, 0, 0, 0]));
  });
});
