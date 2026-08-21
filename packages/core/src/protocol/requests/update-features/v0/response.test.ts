import { describe, expect, it } from 'vitest';
import { KafkaAggregateError, KafkaUpdateFeaturesError } from '../../../../errors';
import { Encoder } from '../../../encoder';
import { responseSchema, updateFeaturesResponseV0 } from './response';

describe('protocol/requests/update-features/v0/response', () => {
  it('decodes feature results and remaps throttleTime', async () => {
    const value = {
      throttleTime: 7,
      errorCode: 0,
      errorMessage: null,
      results: [{ feature: 'metadata.version', errorCode: 0, errorMessage: null }],
    };
    const encoder = new Encoder();
    responseSchema.write(encoder, value);

    const data = await updateFeaturesResponseV0.decode(encoder.buffer);
    expect(data).toEqual({ ...value, throttleTime: 0, clientSideThrottleTime: 7 });
    await expect(updateFeaturesResponseV0.parse(data)).resolves.toEqual(data);
  });

  it('preserves every failed feature and broker message', async () => {
    const parsing = updateFeaturesResponseV0.parse({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      errorMessage: null,
      results: [
        { feature: 'metadata.version', errorCode: 42, errorMessage: 'bad metadata level' },
        { feature: 'kraft.version', errorCode: 42, errorMessage: 'bad kraft level' },
      ],
    });

    const error = await parsing.catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(KafkaAggregateError);
    expect((error as KafkaAggregateError).errors).toEqual([
      expect.objectContaining({
        name: 'KafkaUpdateFeaturesError',
        feature: 'metadata.version',
        message: 'bad metadata level',
      }),
      expect.objectContaining({
        name: 'KafkaUpdateFeaturesError',
        feature: 'kraft.version',
        message: 'bad kraft level',
      }),
    ]);
    expect((error as KafkaAggregateError).errors[0]).toBeInstanceOf(KafkaUpdateFeaturesError);
  });

  it('throws the top-level broker error message before feature errors', async () => {
    await expect(
      updateFeaturesResponseV0.parse({
        throttleTime: 0,
        clientSideThrottleTime: 0,
        errorCode: 31,
        errorMessage: 'cluster policy denied this update',
        results: [{ feature: 'metadata.version', errorCode: 42, errorMessage: 'ignored' }],
      }),
    ).rejects.toMatchObject({
      type: 'CLUSTER_AUTHORIZATION_FAILED',
      message: 'cluster policy denied this update',
    });
  });
});
