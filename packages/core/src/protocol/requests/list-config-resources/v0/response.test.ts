import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { CONFIG_RESOURCE_TYPES } from '../../../enums/config-resource-types';
import { listConfigResourcesResponseV0 } from './response';

function responseFixture(errorCode = 0): Buffer {
  return new Encoder()
    .writeInt32(12)
    .writeInt16(errorCode)
    .writeUVarInt(2)
    .writeUVarIntString('org.apache.kafka.producer')
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/list-config-resources/v0/response', () => {
  it('decodes client-metrics names and defaults resourceType to 16', async () => {
    await expect(listConfigResourcesResponseV0.decode(responseFixture())).resolves.toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 12,
      errorCode: 0,
      configResources: [
        {
          resourceName: 'org.apache.kafka.producer',
          resourceType: CONFIG_RESOURCE_TYPES.CLIENT_METRICS,
        },
      ],
    });
  });

  it('rejects a top-level protocol error', async () => {
    const data = await listConfigResourcesResponseV0.decode(responseFixture(31));
    await expect(listConfigResourcesResponseV0.parse(data)).rejects.toMatchObject({
      type: 'CLUSTER_AUTHORIZATION_FAILED',
    });
  });
});
