import { describe, expect, it } from 'vitest';
import v1RequestFixture from '../fixtures/v1-request.json' with { type: 'json' };
import { CONFIG_RESOURCE_TYPES } from '../../../enums/config-resource-types.js';
import { describeConfigsRequestV2 } from './request.js';
import { withDefaultConfigNames } from '../v1/request.js';

describe('protocol/requests/describe-configs/v2/request', () => {
  it('encodes identically to v1 (wire format is unchanged)', async () => {
    const definition = describeConfigsRequestV2({
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
