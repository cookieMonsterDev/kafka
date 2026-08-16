import { describe, expect, it } from 'vitest';
import { CONFIG_RESOURCE_TYPES } from '../../../enums/config-resource-types';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { withDefaultConfigNames } from '../v1/request';
import { describeConfigsRequestV0 } from './request';

describe('protocol/requests/describe-configs/v0/request', () => {
  it('encodes to match the real fixture', async () => {
    const definition = describeConfigsRequestV0({
      resources: withDefaultConfigNames([
        {
          type: CONFIG_RESOURCE_TYPES.TOPIC,
          name: 'test-topic-332d38bc4eee2ff29df6',
          configNames: ['compression.type', 'retention.ms'],
        },
      ]),
    });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
