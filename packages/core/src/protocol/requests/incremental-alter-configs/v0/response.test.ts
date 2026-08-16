import { describe, expect, it } from 'vitest';
import { KafkaProtocolError } from '../../../../errors';
import { Encoder } from '../../../encoder';
import { CONFIG_RESOURCE_TYPES } from '../../../enums/config-resource-types';
import { incrementalAlterConfigsResponseV0, responseSchema } from './response';

describe('protocol/requests/incremental-alter-configs/v0/response', () => {
  it('round-trips a successful v0 response', async () => {
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
    const data = await incrementalAlterConfigsResponseV0.decode(encoder.buffer);
    expect(data).toEqual(value);
    await expect(incrementalAlterConfigsResponseV0.parse(data)).resolves.toEqual(data);
  });

  it('throws the first resource error from parse', async () => {
    const value = {
      throttleTime: 0,
      resources: [
        {
          errorCode: 31, // INVALID_CONFIG
          errorMessage: 'Invalid config',
          resourceType: CONFIG_RESOURCE_TYPES.TOPIC,
          resourceName: 'orders',
        },
      ],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await incrementalAlterConfigsResponseV0.decode(encoder.buffer);
    await expect(incrementalAlterConfigsResponseV0.parse(data)).rejects.toBeInstanceOf(KafkaProtocolError);
  });
});
