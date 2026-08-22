import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { unregisterBrokerResponseV0, responseSchema } from './response';

describe('protocol/requests/unregister-broker/v0/response', () => {
  it('round-trips a flexible v0 response and remaps throttleTime', async () => {
    const value = {
      throttleTime: 5,
      errorCode: 0,
      errorMessage: null,
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await unregisterBrokerResponseV0.decode(encoder.buffer);
    expect(data).toEqual({
      ...value,
      throttleTime: 0,
      clientSideThrottleTime: 5,
    });
    await expect(unregisterBrokerResponseV0.parse(data)).resolves.toEqual(data);
  });

  it('throws on a top-level error code', async () => {
    await expect(
      unregisterBrokerResponseV0.parse({
        throttleTime: 0,
        clientSideThrottleTime: 0,
        errorCode: 3,
        errorMessage: 'unknown broker',
      }),
    ).rejects.toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION' });
  });
});
