import { KafkaNonRetriableError } from '../../../errors';
import type { ProtocolFactory, RequestFamily } from '../index';
import { updateFeaturesRequestV0 } from './v0/request';
import { updateFeaturesResponseV0 } from './v0/response';
import { updateFeaturesRequestV1 } from './v1/request';
import { updateFeaturesResponseV1 } from './v1/response';
import { updateFeaturesRequestV2 } from './v2/request';
import { updateFeaturesResponseV2 } from './v2/response';

export interface UpdateFeatureInput {
  feature: string;
  maxVersionLevel: number;
  upgradeType: number;
}

export interface UpdateFeaturesOptions {
  featureUpdates: UpdateFeatureInput[];
  timeout?: number;
  validateOnly?: boolean;
}

const DEFAULT_TIMEOUT_MS = 60_000;

const modernFields = (options: UpdateFeaturesOptions) => ({
  timeoutMs: options.timeout ?? DEFAULT_TIMEOUT_MS,
  featureUpdates: options.featureUpdates,
  validateOnly: options.validateOnly ?? false,
});

const VERSIONS: Readonly<Record<number, ProtocolFactory<UpdateFeaturesOptions>>> = {
  0: (options) => {
    if (options.validateOnly === true) {
      throw new KafkaNonRetriableError(
        'UpdateFeatures v0 does not support validateOnly; this broker needs UpdateFeatures v1 or newer',
      );
    }
    if (options.featureUpdates.some(({ upgradeType }) => upgradeType === 3)) {
      throw new KafkaNonRetriableError(
        'UpdateFeatures v0 does not support unsafe downgrades; this broker needs UpdateFeatures v1 or newer',
      );
    }
    return {
      request: updateFeaturesRequestV0({
        timeoutMs: options.timeout ?? DEFAULT_TIMEOUT_MS,
        featureUpdates: options.featureUpdates.map(({ feature, maxVersionLevel, upgradeType }) => ({
          feature,
          maxVersionLevel,
          allowDowngrade: upgradeType !== 1,
        })),
      }),
      response: updateFeaturesResponseV0,
    };
  },
  1: (options) => ({
    request: updateFeaturesRequestV1(modernFields(options)),
    response: updateFeaturesResponseV1,
  }),
  2: (options) => ({
    request: updateFeaturesRequestV2(modernFields(options)),
    response: updateFeaturesResponseV2,
  }),
};

export const UpdateFeatures: RequestFamily<UpdateFeaturesOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no UpdateFeatures protocol for version ${version}`);
    return factory;
  },
});
