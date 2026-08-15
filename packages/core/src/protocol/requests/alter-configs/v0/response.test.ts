import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { CONFIG_RESOURCE_TYPES } from '../../../enums/config-resource-types.js';
import { alterConfigsResponseV0 } from './response.js';

describe('protocol/requests/alter-configs/v0/response', () => {
  it('decodes a real fixture', async () => {
    const data = await alterConfigsResponseV0.decode(Buffer.from(v0ResponseFixture.data));

    expect(data).toEqual({
      throttleTime: 0,
      resources: [
        {
          errorCode: 0,
          errorMessage: null,
          resourceName: 'test-topic-d7fa92c03177d87573b1-38076-21364f66-8613-47e0-b273-bc9de397515e',
          resourceType: CONFIG_RESOURCE_TYPES.TOPIC,
        },
      ],
    });

    await expect(alterConfigsResponseV0.parse(data)).resolves.toBeTruthy();
  });
});
