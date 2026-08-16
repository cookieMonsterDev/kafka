import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { INCREMENTAL_ALTER_CONFIGS_OPERATIONS } from '../../../enums/incremental-alter-configs-operations';
import { CONFIG_RESOURCE_TYPES } from '../../../enums/config-resource-types';
import { incrementalAlterConfigsRequestV0, requestSchema } from './request';

describe('protocol/requests/incremental-alter-configs/v0/request', () => {
  it('round-trips a v0 request', async () => {
    const value = {
      resources: [
        {
          type: CONFIG_RESOURCE_TYPES.TOPIC,
          name: 'orders',
          configs: [
            { name: 'cleanup.policy', operation: INCREMENTAL_ALTER_CONFIGS_OPERATIONS.SET, value: 'compact' },
            {
              name: 'unclean.leader.election.enable',
              operation: INCREMENTAL_ALTER_CONFIGS_OPERATIONS.DELETE,
              value: null,
            },
          ],
        },
      ],
      validateOnly: false,
    };

    const encoder = await incrementalAlterConfigsRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
