import { Decoder } from '../../decoder';
import { compactArray, compactString, field, flexibleObject, int16 } from '../../schema';

export interface SupportedFeature {
  name: string;
  minVersion: number;
  maxVersion: number;
}

export interface FinalizedFeature {
  name: string;
  maxVersionLevel: number;
  minVersionLevel: number;
}

export interface ApiVersionsFeatureFields {
  supportedFeatures: SupportedFeature[];
  finalizedFeaturesEpoch: bigint;
  finalizedFeatures: FinalizedFeature[];
  zkMigrationReady: boolean | null;
}

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

const supportedFeaturesSchema = compactArray(supportedFeatureSchema);
const finalizedFeaturesSchema = compactArray(finalizedFeatureSchema);

/**
 * Parses ApiVersions v3+ body tagged fields (KIP-584 / KIP-482).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export function readApiVersionsFeatureFields(decoder: Decoder): ApiVersionsFeatureFields {
  const supportedFeatures: SupportedFeature[] = [];
  let finalizedFeaturesEpoch = -1n;
  const finalizedFeatures: FinalizedFeature[] = [];
  let zkMigrationReady: boolean | null = null;

  const numberOfTaggedFields = decoder.readUVarInt();
  for (let i = 0; i < numberOfTaggedFields; i++) {
    const tag = decoder.readUVarInt();
    const size = decoder.readUVarInt();
    const fieldDecoder = decoder.slice(size);
    decoder.forward(size);

    switch (tag) {
      case 0:
        supportedFeatures.push(...supportedFeaturesSchema.read(fieldDecoder));
        break;
      case 1:
        finalizedFeaturesEpoch = fieldDecoder.readInt64();
        break;
      case 2:
        finalizedFeatures.push(...finalizedFeaturesSchema.read(fieldDecoder));
        break;
      case 3:
        zkMigrationReady = fieldDecoder.readInt8() !== 0;
        break;
      default:
        break;
    }
  }

  return { supportedFeatures, finalizedFeaturesEpoch, finalizedFeatures, zkMigrationReady };
}
