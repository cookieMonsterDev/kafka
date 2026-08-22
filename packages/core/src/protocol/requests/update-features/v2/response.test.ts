import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { responseSchema, updateFeaturesResponseV2 } from './response';

describe('protocol/requests/update-features/v2/response', () => {
  it('decodes the v2 response without per-feature results', async () => {
    const value = { throttleTime: 9, errorCode: 0, errorMessage: null };
    const encoder = new Encoder();
    responseSchema.write(encoder, value);

    const data = await updateFeaturesResponseV2.decode(encoder.buffer);
    expect(data).toEqual({
      ...value,
      throttleTime: 0,
      clientSideThrottleTime: 9,
      results: [],
    });
    await expect(updateFeaturesResponseV2.parse(data)).resolves.toEqual(data);
  });

  it('preserves a top-level broker error message', async () => {
    await expect(
      updateFeaturesResponseV2.parse({
        throttleTime: 0,
        clientSideThrottleTime: 0,
        errorCode: 42,
        errorMessage: 'feature dependency failed',
        results: [],
      }),
    ).rejects.toMatchObject({ type: 'INVALID_REQUEST', message: 'feature dependency failed' });
  });
});
