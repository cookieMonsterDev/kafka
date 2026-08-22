import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { requestSchema, updateFeaturesRequestV0 } from './request';

describe('protocol/requests/update-features/v0/request', () => {
  it('encodes the deprecated allowDowngrade field', async () => {
    const value = {
      timeoutMs: 60_000,
      featureUpdates: [{ feature: 'metadata.version', maxVersionLevel: 20, allowDowngrade: true }],
    };

    const encoder = await updateFeaturesRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
