import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { INCREMENTAL_ALTER_CONFIGS_OPERATIONS } from '../../../enums/incremental-alter-configs-operations';
import { CONFIG_RESOURCE_TYPES } from '../../../enums/config-resource-types';
import { incrementalAlterConfigsRequestV1, requestSchema } from './request';

describe('protocol/requests/incremental-alter-configs/v1/request', () => {
  it('round-trips a flexible v1 request', async () => {
    const value = {
      resources: [
        {
          type: CONFIG_RESOURCE_TYPES.TOPIC,
          name: 'orders',
          configs: [{ name: 'cleanup.policy', operation: INCREMENTAL_ALTER_CONFIGS_OPERATIONS.SET, value: 'compact' }],
        },
      ],
      validateOnly: true,
    };

    const encoder = await incrementalAlterConfigsRequestV1(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
