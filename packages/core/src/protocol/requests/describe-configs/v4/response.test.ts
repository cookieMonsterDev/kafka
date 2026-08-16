import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { CONFIG_RESOURCE_TYPES } from '../../../enums/config-resource-types';
import { CONFIG_SOURCE } from '../../../enums/config-source';
import { CONFIG_TYPE } from '../../../enums/config-type';
import { describeConfigsResponseV4 } from './response';

function encodeV4Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeUVarInt(2)
    .writeInt16(0)
    .writeUVarIntString(null)
    .writeInt8(CONFIG_RESOURCE_TYPES.TOPIC)
    .writeUVarIntString('topic-test1')
    .writeUVarInt(2)
    .writeUVarIntString('compression.type')
    .writeUVarIntString('producer')
    .writeBoolean(false)
    .writeInt8(CONFIG_SOURCE.DEFAULT_CONFIG)
    .writeBoolean(false)
    .writeUVarInt(2)
    .writeUVarIntString('compression.type')
    .writeUVarIntString('producer')
    .writeInt8(CONFIG_SOURCE.DEFAULT_CONFIG)
    .writeUVarInt(0)
    .writeInt8(CONFIG_TYPE.STRING)
    .writeUVarIntString('The compression type for a given topic')
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/describe-configs/v4/response', () => {
  it('decodes a flexible body, deriving isDefault and remapping throttleTime', async () => {
    const data = await describeConfigsResponseV4.decode(encodeV4Response({ throttleTime: 8 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      resources: [
        {
          errorCode: 0,
          errorMessage: null,
          resourceType: CONFIG_RESOURCE_TYPES.TOPIC,
          resourceName: 'topic-test1',
          configEntries: [
            {
              configName: 'compression.type',
              configValue: 'producer',
              readOnly: false,
              isDefault: true,
              configSource: CONFIG_SOURCE.DEFAULT_CONFIG,
              isSensitive: false,
              configSynonyms: [
                {
                  configName: 'compression.type',
                  configValue: 'producer',
                  configSource: CONFIG_SOURCE.DEFAULT_CONFIG,
                },
              ],
              configType: CONFIG_TYPE.STRING,
              documentation: 'The compression type for a given topic',
            },
          ],
        },
      ],
    });

    await expect(describeConfigsResponseV4.parse(data)).resolves.toBe(data);
  });
});
