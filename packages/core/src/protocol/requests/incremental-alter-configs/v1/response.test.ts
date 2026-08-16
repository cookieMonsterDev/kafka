import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { CONFIG_RESOURCE_TYPES } from '../../../enums/config-resource-types';
import { incrementalAlterConfigsResponseV1, responseSchema } from './response';

describe('protocol/requests/incremental-alter-configs/v1/response', () => {
  it('round-trips a flexible v1 response', async () => {
    const value = {
      throttleTime: 0,
      resources: [
        {
          errorCode: 0,
          errorMessage: null,
          resourceType: CONFIG_RESOURCE_TYPES.TOPIC,
          resourceName: 'orders',
        },
      ],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await incrementalAlterConfigsResponseV1.decode(encoder.buffer);
    expect(data).toEqual(value);
    await expect(incrementalAlterConfigsResponseV1.parse(data)).resolves.toEqual(data);
  });
});
