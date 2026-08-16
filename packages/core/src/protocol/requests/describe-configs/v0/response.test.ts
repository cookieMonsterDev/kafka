import { describe, expect, it } from 'vitest';
import { CONFIG_SOURCE } from '../../../enums/config-source';
import v0ResponseBrokerConfigFixture from '../fixtures/v0-response-broker-config.json' with { type: 'json' };
import v0ResponseTopicConfigFixture from '../fixtures/v0-response-topic-config.json' with { type: 'json' };
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { describeConfigsResponseV0 } from './response';

describe('protocol/requests/describe-configs/v0/response', () => {
  it('decodes a real fixture, back-porting configSource from isDefault', async () => {
    const data = await describeConfigsResponseV0.decode(Buffer.from(v0ResponseFixture.data));

    expect(data).toEqual({
      throttleTime: 0,
      resources: [
        {
          errorCode: 0,
          errorMessage: null,
          resourceType: 2,
          resourceName: 'test-topic-443a0ba6d66fd2161c73',
          configEntries: [
            {
              configName: 'compression.type',
              configValue: 'producer',
              readOnly: false,
              isDefault: true,
              configSource: CONFIG_SOURCE.DEFAULT_CONFIG,
              isSensitive: false,
              configSynonyms: [],
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

    await expect(describeConfigsResponseV0.parse(data)).resolves.toBeTruthy();
  });

  it('sets TOPIC_CONFIG when a non-default entry belongs to a topic resource', async () => {
    const data = await describeConfigsResponseV0.decode(Buffer.from(v0ResponseTopicConfigFixture.data));
    const [resource] = data.resources;
    expect(resource?.configEntries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          configName: 'retention.ms',
          isDefault: false,
          configSource: CONFIG_SOURCE.TOPIC_CONFIG,
        }),
      ]),
    );
  });

  it('sets STATIC_BROKER_CONFIG when a non-default entry belongs to a broker resource', async () => {
    const data = await describeConfigsResponseV0.decode(Buffer.from(v0ResponseBrokerConfigFixture.data));
    const [resource] = data.resources;
    expect(resource?.configEntries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          configName: 'sasl.kerberos.service.name',
          isDefault: false,
          configSource: CONFIG_SOURCE.STATIC_BROKER_CONFIG,
        }),
      ]),
    );
  });
});
