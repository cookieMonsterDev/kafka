import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { CONFIG_RESOURCE_TYPES } from '../../../enums/config-resource-types';
import { alterConfigsRequestV1 } from '../v1/request';
import { alterConfigsRequestV2, requestSchema } from './request';

const payload = {
  resources: [
    {
      type: CONFIG_RESOURCE_TYPES.TOPIC,
      name: 'orders',
      configEntries: [{ name: 'cleanup.policy', value: 'compact' }],
    },
  ],
  validateOnly: false,
};

describe('protocol/requests/alter-configs/v2/request', () => {
  it('round-trips a flexible v2 request', async () => {
    const definition = alterConfigsRequestV2(payload);
    expect(definition.apiVersion).toBe(2);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarInt(2)
      .writeInt8(CONFIG_RESOURCE_TYPES.TOPIC)
      .writeUVarIntString('orders')
      .writeUVarInt(2)
      .writeUVarIntString('cleanup.policy')
      .writeUVarIntString('compact')
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeBoolean(false)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the non-flexible v1 encoding', async () => {
    const v2 = await alterConfigsRequestV2(payload).encode();
    const v1 = await alterConfigsRequestV1(payload).encode();
    expect(v2.buffer).not.toEqual(v1.buffer);
  });
});
