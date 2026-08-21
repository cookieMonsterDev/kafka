import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { requestSchema, updateFeaturesRequestV1 } from './request';

describe('protocol/requests/update-features/v1/request', () => {
  it('encodes upgradeType and validateOnly', async () => {
    const value = {
      timeoutMs: 1234,
      featureUpdates: [{ feature: 'metadata.version', maxVersionLevel: 19, upgradeType: 2 }],
      validateOnly: true,
    };

    const encoder = await updateFeaturesRequestV1(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
