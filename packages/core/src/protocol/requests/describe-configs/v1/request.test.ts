import { describe, expect, it } from 'vitest';
import v1RequestFixture from '../fixtures/v1-request.json' with { type: 'json' };
import { CONFIG_RESOURCE_TYPES } from '../../../enums/config-resource-types.js';
import { describeConfigsRequestV1, withDefaultConfigNames } from './request.js';

describe('protocol/requests/describe-configs/v1/request', () => {
  it('encodes to match the real fixture', async () => {
    const definition = describeConfigsRequestV1({
      includeSynonyms: true,
      resources: withDefaultConfigNames([
        {
          type: CONFIG_RESOURCE_TYPES.TOPIC,
          name: 'topic-test1',
          configNames: ['compression.type', 'retention.ms'],
        },
      ]),
    });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v1RequestFixture.data));
  });
});
