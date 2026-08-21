import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { CONFIG_RESOURCE_TYPES } from '../protocol/enums/config-resource-types';
import { createConfigsApi } from './configs';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function fakeCluster(
  listConfigResources = vi.fn().mockResolvedValue({
    configResources: [{ resourceName: 'orders', resourceType: CONFIG_RESOURCE_TYPES.TOPIC }],
  }),
) {
  const broker = { listConfigResources };
  return {
    refreshMetadata: vi.fn().mockResolvedValue(undefined),
    findControllerBroker: vi.fn().mockResolvedValue(broker),
    broker,
  };
}

describe('admin/configs', () => {
  it('lists config resources through the active controller', async () => {
    const cluster = fakeCluster();
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(api.listConfigResources()).resolves.toEqual({
      resources: [{ resourceName: 'orders', resourceType: CONFIG_RESOURCE_TYPES.TOPIC }],
    });

    expect(cluster.refreshMetadata).toHaveBeenCalled();
    expect(cluster.findControllerBroker).toHaveBeenCalled();
    expect(cluster.broker.listConfigResources).toHaveBeenCalledWith({ resourceTypes: undefined });
  });

  it('forwards resourceTypes to the controller broker', async () => {
    const cluster = fakeCluster();
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await api.listConfigResources({ resourceTypes: [CONFIG_RESOURCE_TYPES.TOPIC] });

    expect(cluster.broker.listConfigResources).toHaveBeenCalledWith({
      resourceTypes: [CONFIG_RESOURCE_TYPES.TOPIC],
    });
  });

  it('rejects unknown resource types before contacting the broker', async () => {
    const cluster = fakeCluster();
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(api.listConfigResources({ resourceTypes: [99] })).rejects.toThrow(KafkaNonRetriableError);
    expect(cluster.findControllerBroker).not.toHaveBeenCalled();
  });

  it('allows an empty resourceTypes list', async () => {
    const cluster = fakeCluster(vi.fn().mockResolvedValue({ configResources: [] }));
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(api.listConfigResources({ resourceTypes: [] })).resolves.toEqual({ resources: [] });
    expect(cluster.broker.listConfigResources).toHaveBeenCalledWith({ resourceTypes: [] });
  });
});
