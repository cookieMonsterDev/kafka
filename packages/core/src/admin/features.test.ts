import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createFeaturesApi } from './features';
import { FEATURE_UPDATE_UPGRADE_TYPES } from './types';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function fakeCluster(updateFeatures = vi.fn().mockResolvedValue({ results: [] })) {
  const broker = { updateFeatures };
  return {
    refreshMetadata: vi.fn().mockResolvedValue(undefined),
    findControllerBroker: vi.fn().mockResolvedValue(broker),
    broker,
  };
}

describe('admin/features', () => {
  it('targets the active controller and applies defaults', async () => {
    const cluster = fakeCluster();
    const api = createFeaturesApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(
      api.updateFeatures({
        featureUpdates: [{ feature: 'metadata.version', maxVersionLevel: 20 }],
      }),
    ).resolves.toEqual({ results: [] });

    expect(cluster.refreshMetadata).toHaveBeenCalled();
    expect(cluster.findControllerBroker).toHaveBeenCalled();
    expect(cluster.broker.updateFeatures).toHaveBeenCalledWith({
      featureUpdates: [
        {
          feature: 'metadata.version',
          maxVersionLevel: 20,
          upgradeType: FEATURE_UPDATE_UPGRADE_TYPES.UPGRADE,
        },
      ],
      timeout: undefined,
      validateOnly: undefined,
    });
  });

  it('returns successful per-feature results from v0/v1 brokers', async () => {
    const results = [{ feature: 'metadata.version', errorCode: 0, errorMessage: null }];
    const cluster = fakeCluster(vi.fn().mockResolvedValue({ results }));
    const api = createFeaturesApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(
      api.updateFeatures({
        featureUpdates: [
          {
            feature: 'metadata.version',
            maxVersionLevel: 19,
            upgradeType: FEATURE_UPDATE_UPGRADE_TYPES.SAFE_DOWNGRADE,
          },
        ],
        validateOnly: true,
      }),
    ).resolves.toEqual({ results });
  });

  it('rejects empty, duplicate, and out-of-range feature updates', async () => {
    const cluster = fakeCluster();
    const api = createFeaturesApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(api.updateFeatures({ featureUpdates: [] })).rejects.toThrow(KafkaNonRetriableError);
    await expect(
      api.updateFeatures({
        featureUpdates: [
          { feature: 'metadata.version', maxVersionLevel: 19 },
          { feature: 'metadata.version', maxVersionLevel: 20 },
        ],
      }),
    ).rejects.toThrow('Duplicate feature update');
    await expect(
      api.updateFeatures({
        featureUpdates: [{ feature: 'metadata.version', maxVersionLevel: 32_768 }],
      }),
    ).rejects.toThrow('Invalid maxVersionLevel');
    expect(cluster.findControllerBroker).not.toHaveBeenCalled();
  });
});
