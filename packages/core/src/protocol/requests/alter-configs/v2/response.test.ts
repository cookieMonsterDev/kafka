import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { CONFIG_RESOURCE_TYPES } from '../../../enums/config-resource-types';
import { alterConfigsResponseV2 } from './response';

function encodeV2Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeUVarInt(2)
    .writeInt16(0)
    .writeUVarIntString(null)
    .writeInt8(CONFIG_RESOURCE_TYPES.TOPIC)
    .writeUVarIntString('orders')
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/alter-configs/v2/response', () => {
  it('decodes a flexible body and remaps throttleTime', async () => {
    const data = await alterConfigsResponseV2.decode(encodeV2Response({ throttleTime: 11 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 11,
      resources: [
        {
          errorCode: 0,
          errorMessage: null,
          resourceType: CONFIG_RESOURCE_TYPES.TOPIC,
          resourceName: 'orders',
        },
      ],
    });
    await expect(alterConfigsResponseV2.parse(data)).resolves.toBe(data);
  });
});
