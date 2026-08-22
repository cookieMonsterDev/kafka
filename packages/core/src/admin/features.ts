import { KafkaNonRetriableError } from '../errors';
import { retrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown, protocolType } from './helpers';
import {
  FEATURE_UPDATE_UPGRADE_TYPES,
  type FeatureUpdateUpgradeType,
  type UpdateFeaturesOptions,
  type UpdateFeaturesResult,
} from './types';

export interface FeaturesApi {
  updateFeatures: (options: UpdateFeaturesOptions) => Promise<{ results: UpdateFeaturesResult[] }>;
}

const VALID_UPGRADE_TYPES: readonly FeatureUpdateUpgradeType[] = Object.values(FEATURE_UPDATE_UPGRADE_TYPES);

export function createFeaturesApi({ cluster, logger, retry }: AdminContext): FeaturesApi {
  const updateFeatures = async ({
    featureUpdates,
    timeout,
    validateOnly,
  }: UpdateFeaturesOptions): Promise<{ results: UpdateFeaturesResult[] }> => {
    if (!Array.isArray(featureUpdates)) {
      throw new KafkaNonRetriableError(`Invalid featureUpdates array ${formatUnknown(featureUpdates)}`);
    }
    if (featureUpdates.length === 0) {
      throw new KafkaNonRetriableError('Feature updates array cannot be empty');
    }
    if (timeout != null && (!Number.isInteger(timeout) || timeout < 0 || timeout > 2_147_483_647)) {
      throw new KafkaNonRetriableError(`Invalid UpdateFeatures timeout ${formatUnknown(timeout)}`);
    }

    const names = new Set<string>();
    const normalized = featureUpdates.map((update) => {
      if (typeof update.feature !== 'string' || update.feature.length === 0) {
        throw new KafkaNonRetriableError(`Invalid feature name ${formatUnknown(update.feature)}`);
      }
      if (names.has(update.feature)) {
        throw new KafkaNonRetriableError(`Duplicate feature update for ${update.feature}`);
      }
      names.add(update.feature);

      if (
        !Number.isInteger(update.maxVersionLevel) ||
        update.maxVersionLevel < -32_768 ||
        update.maxVersionLevel > 32_767
      ) {
        throw new KafkaNonRetriableError(
          `Invalid maxVersionLevel for ${update.feature}: ${formatUnknown(update.maxVersionLevel)}`,
        );
      }
      const upgradeType = update.upgradeType ?? FEATURE_UPDATE_UPGRADE_TYPES.UPGRADE;
      if (!VALID_UPGRADE_TYPES.includes(upgradeType)) {
        throw new KafkaNonRetriableError(
          `Invalid upgradeType for ${update.feature}: ${formatUnknown(update.upgradeType)}`,
        );
      }
      return { ...update, upgradeType };
    });

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        const { results } = await broker.updateFeatures({
          featureUpdates: normalized,
          timeout,
          validateOnly,
        });
        return { results };
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not update finalized features', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          await cluster.refreshMetadata();
          throw error;
        }
        bail(error as Error);
        throw error;
      }
    });
  };

  return { updateFeatures };
}
