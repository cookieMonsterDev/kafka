import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { CONFIG_RESOURCE_TYPES } from '../../../enums/config-resource-types';
import { listConfigResourcesRequestV1, requestSchema } from './request';

describe('protocol/requests/list-config-resources/v1/request', () => {
  it('encodes resource types as a compact int8 array', async () => {
    const value = {
      resourceTypes: [CONFIG_RESOURCE_TYPES.TOPIC, CONFIG_RESOURCE_TYPES.BROKER],
    };
    const encoder = await listConfigResourcesRequestV1(value).encode();
    const expected = new Encoder()
      .writeUVarInt(3)
      .writeInt8(CONFIG_RESOURCE_TYPES.TOPIC)
      .writeInt8(CONFIG_RESOURCE_TYPES.BROKER)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });

  it('encodes an empty resource type array', async () => {
    const encoder = await listConfigResourcesRequestV1({ resourceTypes: [] }).encode();
    expect(encoder.buffer).toEqual(Buffer.from([1, 0]));
  });
});
