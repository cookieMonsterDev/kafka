import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { Encoder } from '../../encoder';
import { compactArray, compactString, field, flexibleObject, int16 } from '../../schema';
import { readApiVersionsFeatureFields } from './feature-fields';

const supportedFeatureSchema = flexibleObject([
  field('name', compactString),
  field('minVersion', int16),
  field('maxVersion', int16),
]);

const finalizedFeatureSchema = flexibleObject([
  field('name', compactString),
  field('maxVersionLevel', int16),
  field('minVersionLevel', int16),
]);

describe('protocol/requests/api-versions/feature-fields', () => {
  it('returns defaults when there are no tagged fields', () => {
    const buffer = new Encoder().writeUVarInt(0).buffer;
    const result = readApiVersionsFeatureFields(new Decoder(buffer));

    expect(result).toEqual({
      supportedFeatures: [],
      finalizedFeaturesEpoch: -1n,
      finalizedFeatures: [],
      zkMigrationReady: null,
    });
  });

  it('parses supported and finalized feature tags', () => {
    const supportedPayload = new Encoder();
    compactArray(supportedFeatureSchema).write(supportedPayload, [
      { name: 'metadata.version', minVersion: 1, maxVersion: 16 },
      { name: 'test.feature', minVersion: 0, maxVersion: 1 },
    ]);

    const finalizedPayload = new Encoder();
    compactArray(finalizedFeatureSchema).write(finalizedPayload, [
      { name: 'metadata.version', maxVersionLevel: 16, minVersionLevel: 16 },
    ]);

    const epochPayload = new Encoder().writeInt64(42n).buffer;
    const zkPayload = Buffer.from([1]);

    const buffer = new Encoder()
      .writeUVarInt(4)
      .writeUVarInt(0)
      .writeUVarInt(supportedPayload.buffer.length)
      .writeBuffer(supportedPayload.buffer)
      .writeUVarInt(1)
      .writeUVarInt(epochPayload.length)
      .writeBuffer(epochPayload)
      .writeUVarInt(2)
      .writeUVarInt(finalizedPayload.buffer.length)
      .writeBuffer(finalizedPayload.buffer)
      .writeUVarInt(3)
      .writeUVarInt(zkPayload.length)
      .writeBuffer(zkPayload).buffer;

    const result = readApiVersionsFeatureFields(new Decoder(buffer));

    expect(result.supportedFeatures).toEqual([
      { name: 'metadata.version', minVersion: 1, maxVersion: 16 },
      { name: 'test.feature', minVersion: 0, maxVersion: 1 },
    ]);
    expect(result.finalizedFeaturesEpoch).toBe(42n);
    expect(result.finalizedFeatures).toEqual([{ name: 'metadata.version', maxVersionLevel: 16, minVersionLevel: 16 }]);
    expect(result.zkMigrationReady).toBe(true);
  });
});
