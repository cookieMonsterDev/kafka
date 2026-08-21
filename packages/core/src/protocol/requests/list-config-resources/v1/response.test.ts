import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { CONFIG_RESOURCE_TYPES } from '../../../enums/config-resource-types';
import { listConfigResourcesResponseV1 } from './response';

function responseFixture(errorCode = 0): Buffer {
  return new Encoder()
    .writeInt32(12)
    .writeInt16(errorCode)
    .writeUVarInt(2)
    .writeUVarIntString('orders')
    .writeInt8(CONFIG_RESOURCE_TYPES.TOPIC)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/list-config-resources/v1/response', () => {
  it('decodes resource name and type', async () => {
    await expect(listConfigResourcesResponseV1.decode(responseFixture())).resolves.toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 12,
      errorCode: 0,
      configResources: [{ resourceName: 'orders', resourceType: CONFIG_RESOURCE_TYPES.TOPIC }],
    });
  });

  it('rejects a top-level protocol error', async () => {
    const data = await listConfigResourcesResponseV1.decode(responseFixture(31));
    await expect(listConfigResourcesResponseV1.parse(data)).rejects.toMatchObject({
      type: 'CLUSTER_AUTHORIZATION_FAILED',
    });
  });
});
