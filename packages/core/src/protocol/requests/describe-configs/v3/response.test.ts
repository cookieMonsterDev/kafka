import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { CONFIG_RESOURCE_TYPES } from '../../../enums/config-resource-types';
import { CONFIG_SOURCE } from '../../../enums/config-source';
import { CONFIG_TYPE } from '../../../enums/config-type';
import { describeConfigsResponseV3 } from './response';

function encodeV3Response(options: {
  throttleTime: number;
  configSource: number;
  configType: number;
  documentation: string | null;
}): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeInt32(1)
    .writeInt16(0)
    .writeString(null)
    .writeInt8(CONFIG_RESOURCE_TYPES.TOPIC)
    .writeString('topic-test1')
    .writeInt32(2)
    .writeString('compression.type')
    .writeString('producer')
    .writeBoolean(false)
    .writeInt8(options.configSource)
    .writeBoolean(false)
    .writeInt32(1)
    .writeString('compression.type')
    .writeString('producer')
    .writeInt8(options.configSource)
    .writeInt8(options.configType)
    .writeString(options.documentation)
    .writeString('retention.ms')
    .writeString('604800000')
    .writeBoolean(false)
    .writeInt8(CONFIG_SOURCE.TOPIC_CONFIG)
    .writeBoolean(false)
    .writeInt32(0)
    .writeInt8(CONFIG_TYPE.LONG)
    .writeString(null).buffer;
}

describe('protocol/requests/describe-configs/v3/response', () => {
  it('decodes configType and documentation, deriving isDefault from configSource', async () => {
    const data = await describeConfigsResponseV3.decode(
      encodeV3Response({
        throttleTime: 8,
        configSource: CONFIG_SOURCE.DEFAULT_CONFIG,
        configType: CONFIG_TYPE.STRING,
        documentation: 'The compression type for a given topic',
      }),
    );

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
            {
              configName: 'retention.ms',
              configValue: '604800000',
              readOnly: false,
              isDefault: false,
              configSource: CONFIG_SOURCE.TOPIC_CONFIG,
              isSensitive: false,
              configSynonyms: [],
              configType: CONFIG_TYPE.LONG,
              documentation: null,
            },
          ],
        },
      ],
    });

    await expect(describeConfigsResponseV3.parse(data)).resolves.toBe(data);
  });
});
