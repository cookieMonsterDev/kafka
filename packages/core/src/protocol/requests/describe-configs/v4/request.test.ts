import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { CONFIG_RESOURCE_TYPES } from '../../../enums/config-resource-types';
import { describeConfigsRequestV3 } from '../v3/request';
import { withDefaultConfigNames } from '../v1/request';
import { describeConfigsRequestV4, requestSchema } from './request';

const payload = {
  includeSynonyms: true,
  includeDocumentation: true,
  resources: withDefaultConfigNames([
    {
      type: CONFIG_RESOURCE_TYPES.TOPIC,
      name: 'topic-test1',
      configNames: ['compression.type', 'retention.ms'],
    },
  ]),
};

describe('protocol/requests/describe-configs/v4/request', () => {
  it('encodes compact strings/arrays and a TAG_BUFFER on every struct', async () => {
    const definition = describeConfigsRequestV4(payload);
    expect(definition.apiVersion).toBe(4);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarInt(2)
      .writeInt8(CONFIG_RESOURCE_TYPES.TOPIC)
      .writeUVarIntString('topic-test1')
      .writeUVarInt(3)
      .writeUVarIntString('compression.type')
      .writeUVarIntString('retention.ms')
      .writeUVarInt(0)
      .writeBoolean(true)
      .writeBoolean(true)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('encodes empty configNames as compact null', async () => {
    const encoder = await describeConfigsRequestV4({
      includeSynonyms: false,
      includeDocumentation: false,
      resources: withDefaultConfigNames([{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'orders' }]),
    }).encode();

    const expected = new Encoder()
      .writeUVarInt(2)
      .writeInt8(CONFIG_RESOURCE_TYPES.TOPIC)
      .writeUVarIntString('orders')
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeBoolean(false)
      .writeBoolean(false)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
  });

  it('is not the non-flexible v3 encoding', async () => {
    const v4 = await describeConfigsRequestV4(payload).encode();
    const v3 = await describeConfigsRequestV3(payload).encode();
    expect(v4.buffer).not.toEqual(v3.buffer);
  });
});
