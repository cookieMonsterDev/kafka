import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { unregisterControllerResponseV0, responseSchema } from './response';

describe('protocol/requests/unregister-controller/v0/response', () => {
  it('round-trips a flexible v0 response and remaps throttleTime', async () => {
    const value = {
      throttleTime: 5,
      errorCode: 0,
      errorMessage: null,
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await unregisterControllerResponseV0.decode(encoder.buffer);
    expect(data).toEqual({
      ...value,
      throttleTime: 0,
      clientSideThrottleTime: 5,
    });
    await expect(unregisterControllerResponseV0.parse(data)).resolves.toEqual(data);
  });

  it('throws CONTROLLER_ID_NOT_REGISTERED for an unknown controller id', async () => {
    await expect(
      unregisterControllerResponseV0.parse({
        throttleTime: 0,
        clientSideThrottleTime: 0,
        errorCode: 136,
        errorMessage: 'Controller ID 9 is not currently registered.',
      }),
    ).rejects.toMatchObject({ type: 'CONTROLLER_ID_NOT_REGISTERED', retriable: false });
  });
});
