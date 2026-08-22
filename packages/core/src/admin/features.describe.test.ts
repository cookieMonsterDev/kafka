import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createFeaturesApi } from './features';

const logger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

describe('admin/features describeFeatures', () => {
  it('queries the active controller for ApiVersions feature metadata', async () => {
    const broker = {
      describeFeatures: vi.fn(async () => ({
        supportedFeatures: [{ name: 'metadata.version', minVersion: 1, maxVersion: 16 }],
        finalizedFeatures: [{ name: 'metadata.version', maxVersionLevel: 16, minVersionLevel: 16 }],
        finalizedFeaturesEpoch: 3n,
        zkMigrationReady: false,
      })),
    };
    const findControllerBroker = vi.fn(async () => broker);
    const cluster = {
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
      findControllerBroker,
    } as unknown as Cluster;
    const api = createFeaturesApi({ cluster, logger, rootLogger: logger });

    const result = await api.describeFeatures();

    expect(findControllerBroker).toHaveBeenCalled();
    expect(result.finalizedFeaturesEpoch).toBe(3n);
    expect(result.supportedFeatures[0]?.name).toBe('metadata.version');
  });
});
