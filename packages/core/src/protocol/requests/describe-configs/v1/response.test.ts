import { describe, expect, it } from 'vitest';
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' };
import { CONFIG_SOURCE } from '../../../enums/config-source.js';
import { describeConfigsResponseV1 } from './response.js';

describe('protocol/requests/describe-configs/v1/response', () => {
  it('decodes a real fixture, deriving isDefault from configSource', async () => {
    const data = await describeConfigsResponseV1.decode(Buffer.from(v1ResponseFixture.data));

    expect(data).toEqual({
      throttleTime: 0,
      resources: [
        {
          errorCode: 0,
          errorMessage: null,
          resourceType: 2,
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
                  configSource: CONFIG_SOURCE.DEFAULT_CONFIG,
                  configValue: 'producer',
                },
              ],
            },
            {
              configName: 'retention.ms',
              configValue: '604800000',
              readOnly: false,
              isDefault: true,
              configSource: CONFIG_SOURCE.DEFAULT_CONFIG,
              isSensitive: false,
              configSynonyms: [],
            },
          ],
        },
      ],
    });

    await expect(describeConfigsResponseV1.parse(data)).resolves.toBeTruthy();
  });
});
