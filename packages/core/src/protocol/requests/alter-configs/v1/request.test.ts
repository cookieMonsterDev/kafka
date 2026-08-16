import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { CONFIG_RESOURCE_TYPES } from '../../../enums/config-resource-types';
import { alterConfigsRequestV1 } from './request';

describe('protocol/requests/alter-configs/v1/request', () => {
  it('encodes identically to v0 (wire format is unchanged)', async () => {
    const definition = alterConfigsRequestV1({
      resources: [
        {
          type: CONFIG_RESOURCE_TYPES.TOPIC,
          name: 'test-topic-d7fa92c03177d87573b1-38076-21364f66-8613-47e0-b273-bc9de397515e',
          configEntries: [{ name: 'cleanup.policy', value: 'compact' }],
        },
      ],
      validateOnly: false,
    });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
