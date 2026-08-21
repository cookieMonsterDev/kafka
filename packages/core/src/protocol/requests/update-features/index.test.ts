import { describe, expect, it } from 'vitest';
import { KafkaNonRetriableError } from '../../../errors';
import { Decoder } from '../../decoder';
import { requestSchema as requestSchemaV0 } from './v0/request';
import { UpdateFeatures } from './index';

describe('protocol/requests/update-features', () => {
  it('advertises all Kafka 4.3 versions', () => {
    expect(UpdateFeatures.versions).toEqual([0, 1, 2]);
  });

  it('maps safe downgrades to allowDowngrade on v0', async () => {
    const protocol = UpdateFeatures.protocol({ version: 0 })({
      featureUpdates: [{ feature: 'metadata.version', maxVersionLevel: 18, upgradeType: 2 }],
    });
    const encoder = await protocol.request.encode();

    expect(requestSchemaV0.read(new Decoder(encoder.buffer))).toEqual({
      timeoutMs: 60_000,
      featureUpdates: [{ feature: 'metadata.version', maxVersionLevel: 18, allowDowngrade: true }],
    });
  });

  it('rejects validateOnly on v0 instead of mutating the cluster', () => {
    expect(() =>
      UpdateFeatures.protocol({ version: 0 })({
        featureUpdates: [{ feature: 'metadata.version', maxVersionLevel: 20, upgradeType: 1 }],
        validateOnly: true,
      }),
    ).toThrow(KafkaNonRetriableError);
  });

  it('rejects unsafe downgrades on v0 instead of weakening their semantics', () => {
    expect(() =>
      UpdateFeatures.protocol({ version: 0 })({
        featureUpdates: [{ feature: 'metadata.version', maxVersionLevel: 18, upgradeType: 3 }],
      }),
    ).toThrow(/does not support unsafe downgrades/);
  });
});
