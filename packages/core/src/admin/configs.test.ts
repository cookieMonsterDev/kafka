import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { CONFIG_RESOURCE_TYPES } from '../protocol/enums/config-resource-types';
import { INCREMENTAL_ALTER_CONFIGS_OPERATIONS } from '../protocol/enums/incremental-alter-configs-operations';
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

function fakeAdminCluster(
  overrides: {
    describeConfigs?: ReturnType<typeof vi.fn>;
    alterConfigs?: ReturnType<typeof vi.fn>;
    incrementalAlterConfigs?: ReturnType<typeof vi.fn>;
    brokerNode?: Record<string, ReturnType<typeof vi.fn>>;
  } = {},
) {
  const controller = {
    describeConfigs: overrides.describeConfigs ?? vi.fn().mockResolvedValue({ resources: [] }),
    alterConfigs: overrides.alterConfigs ?? vi.fn().mockResolvedValue({ resources: [] }),
    incrementalAlterConfigs: overrides.incrementalAlterConfigs ?? vi.fn().mockResolvedValue({ resources: [] }),
  };
  return {
    refreshMetadata: vi.fn().mockResolvedValue(undefined),
    findControllerBroker: vi.fn().mockResolvedValue(controller),
    findBroker: vi.fn().mockResolvedValue(overrides.brokerNode ?? controller),
    controller,
  };
}

describe('admin/configs describeConfigs', () => {
  it('rejects a non-array resources input', async () => {
    const cluster = fakeAdminCluster();
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    // @ts-expect-error intentionally invalid input
    await expect(api.describeConfigs({ resources: 'nope' })).rejects.toThrow(KafkaNonRetriableError);
  });

  it('rejects an empty resources array', async () => {
    const cluster = fakeAdminCluster();
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(api.describeConfigs({ resources: [] })).rejects.toThrow('Resources array cannot be empty');
  });

  it('rejects an invalid resource type', async () => {
    const cluster = fakeAdminCluster();
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(api.describeConfigs({ resources: [{ type: 99, name: 'orders' }] })).rejects.toThrow(
      KafkaNonRetriableError,
    );
  });

  it('rejects a missing resource name', async () => {
    const cluster = fakeAdminCluster();
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(api.describeConfigs({ resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: '' }] })).rejects.toThrow(
      KafkaNonRetriableError,
    );
  });

  it('rejects non-array configNames', async () => {
    const cluster = fakeAdminCluster();
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(
      api.describeConfigs({
        // @ts-expect-error intentionally invalid input
        resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'orders', configNames: 'nope' }],
      }),
    ).rejects.toThrow(KafkaNonRetriableError);
  });

  it('routes BROKER resources to the matching broker node and flattens the responses', async () => {
    const brokerNode = { describeConfigs: vi.fn().mockResolvedValue({ resources: [{ resourceName: '1' }] }) };
    const controllerDescribe = vi.fn().mockResolvedValue({ resources: [{ resourceName: 'orders' }] });
    const cluster = fakeAdminCluster({ describeConfigs: controllerDescribe, brokerNode });

    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    const result = await api.describeConfigs({
      resources: [
        { type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'orders' },
        { type: CONFIG_RESOURCE_TYPES.BROKER, name: '1' },
      ],
      includeSynonyms: true,
      includeDocumentation: true,
    });

    expect(cluster.findBroker).toHaveBeenCalledWith({ nodeId: '1' });
    expect(controllerDescribe).toHaveBeenCalledWith({
      resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'orders' }],
      includeSynonyms: true,
      includeDocumentation: true,
    });
    expect(brokerNode.describeConfigs).toHaveBeenCalledWith({
      resources: [{ type: CONFIG_RESOURCE_TYPES.BROKER, name: '1' }],
      includeSynonyms: true,
      includeDocumentation: true,
    });
    expect(result.resources).toEqual(expect.arrayContaining([{ resourceName: 'orders' }, { resourceName: '1' }]));
  });

  it('retries and rethrows on stale metadata errors', async () => {
    const error = Object.assign(new Error('stale'), { type: 'NOT_LEADER_OR_FOLLOWER' });
    const cluster = fakeAdminCluster({ describeConfigs: vi.fn().mockRejectedValue(error) });
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
      retry: { retries: 0 },
    });

    await expect(
      api.describeConfigs({ resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'orders' }] }),
    ).rejects.toThrow();
  });

  it('bails immediately on a non-retriable broker error', async () => {
    const error = new Error('boom');
    const describeConfigs = vi.fn().mockRejectedValue(error);
    const cluster = fakeAdminCluster({ describeConfigs });
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(
      api.describeConfigs({ resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'orders' }] }),
    ).rejects.toThrow('boom');
    expect(describeConfigs).toHaveBeenCalledTimes(1);
  });
});

describe('admin/configs alterConfigs', () => {
  const validEntries = [{ name: 'retention.ms', value: '60000' }];

  it('rejects a non-array resources input', async () => {
    const cluster = fakeAdminCluster();
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    // @ts-expect-error intentionally invalid input
    await expect(api.alterConfigs({ resources: null })).rejects.toThrow(KafkaNonRetriableError);
  });

  it('rejects an empty resources array', async () => {
    const cluster = fakeAdminCluster();
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(api.alterConfigs({ resources: [] })).rejects.toThrow('Resources array cannot be empty');
  });

  it('rejects an invalid resource type', async () => {
    const cluster = fakeAdminCluster();
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(
      api.alterConfigs({ resources: [{ type: 99, name: 'orders', configEntries: validEntries }] }),
    ).rejects.toThrow(KafkaNonRetriableError);
  });

  it('rejects a missing resource name', async () => {
    const cluster = fakeAdminCluster();
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(
      api.alterConfigs({
        resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: '', configEntries: validEntries }],
      }),
    ).rejects.toThrow(KafkaNonRetriableError);
  });

  it('rejects non-array configEntries', async () => {
    const cluster = fakeAdminCluster();
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(
      // @ts-expect-error intentionally invalid input
      api.alterConfigs({ resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'orders' }] }),
    ).rejects.toThrow(KafkaNonRetriableError);
  });

  it('rejects a non-string config entry value', async () => {
    const cluster = fakeAdminCluster();
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(
      api.alterConfigs({
        resources: [
          {
            type: CONFIG_RESOURCE_TYPES.TOPIC,
            name: 'orders',
            // @ts-expect-error intentionally invalid input
            configEntries: [{ name: 'retention.ms', value: 60000 }],
          },
        ],
      }),
    ).rejects.toThrow('Invalid resource config value');
  });

  it('forwards validateOnly and flattens responses across brokers', async () => {
    const alterConfigs = vi.fn().mockResolvedValue({ resources: [{ resourceName: 'orders', errorCode: 0 }] });
    const cluster = fakeAdminCluster({ alterConfigs });
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    const result = await api.alterConfigs({
      resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'orders', configEntries: validEntries }],
      validateOnly: true,
    });

    expect(alterConfigs).toHaveBeenCalledWith({
      resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'orders', configEntries: validEntries }],
      validateOnly: true,
    });
    expect(result).toEqual({ resources: [{ resourceName: 'orders', errorCode: 0 }] });
  });

  it('retries and rethrows on NOT_CONTROLLER', async () => {
    const error = Object.assign(new Error('not controller'), { type: 'NOT_CONTROLLER' });
    const cluster = fakeAdminCluster({ alterConfigs: vi.fn().mockRejectedValue(error) });
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
      retry: { retries: 0 },
    });

    await expect(
      api.alterConfigs({
        resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'orders', configEntries: validEntries }],
      }),
    ).rejects.toThrow();
  });

  it('bails immediately on a non-retriable broker error', async () => {
    const alterConfigs = vi.fn().mockRejectedValue(new Error('boom'));
    const cluster = fakeAdminCluster({ alterConfigs });
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(
      api.alterConfigs({
        resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'orders', configEntries: validEntries }],
      }),
    ).rejects.toThrow('boom');
    expect(alterConfigs).toHaveBeenCalledTimes(1);
  });
});

describe('admin/configs incrementalAlterConfigs', () => {
  const validConfigs = [{ name: 'retention.ms', value: '60000', operation: INCREMENTAL_ALTER_CONFIGS_OPERATIONS.SET }];

  it('rejects a non-array resources input', async () => {
    const cluster = fakeAdminCluster();
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    // @ts-expect-error intentionally invalid input
    await expect(api.incrementalAlterConfigs({ resources: undefined })).rejects.toThrow(KafkaNonRetriableError);
  });

  it('rejects an empty resources array', async () => {
    const cluster = fakeAdminCluster();
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(api.incrementalAlterConfigs({ resources: [] })).rejects.toThrow('Resources array cannot be empty');
  });

  it('rejects an invalid resource type', async () => {
    const cluster = fakeAdminCluster();
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(
      api.incrementalAlterConfigs({ resources: [{ type: 99, name: 'orders', configs: validConfigs }] }),
    ).rejects.toThrow(KafkaNonRetriableError);
  });

  it('rejects a missing resource name', async () => {
    const cluster = fakeAdminCluster();
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(
      api.incrementalAlterConfigs({
        resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: '', configs: validConfigs }],
      }),
    ).rejects.toThrow(KafkaNonRetriableError);
  });

  it('rejects non-array configs', async () => {
    const cluster = fakeAdminCluster();
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(
      // @ts-expect-error intentionally invalid input
      api.incrementalAlterConfigs({ resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'orders' }] }),
    ).rejects.toThrow(KafkaNonRetriableError);
  });

  it('rejects an invalid config operation', async () => {
    const cluster = fakeAdminCluster();
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(
      api.incrementalAlterConfigs({
        resources: [
          {
            type: CONFIG_RESOURCE_TYPES.TOPIC,
            name: 'orders',
            configs: [{ name: 'retention.ms', value: '60000', operation: 99 }],
          },
        ],
      }),
    ).rejects.toThrow('Invalid resource config value');
  });

  it('allows a null config value (delete operation)', async () => {
    const incrementalAlterConfigs = vi.fn().mockResolvedValue({ resources: [] });
    const cluster = fakeAdminCluster({ incrementalAlterConfigs });
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await api.incrementalAlterConfigs({
      resources: [
        {
          type: CONFIG_RESOURCE_TYPES.TOPIC,
          name: 'orders',
          configs: [{ name: 'retention.ms', value: null, operation: INCREMENTAL_ALTER_CONFIGS_OPERATIONS.DELETE }],
        },
      ],
    });

    expect(incrementalAlterConfigs).toHaveBeenCalledWith({
      resources: [
        {
          type: CONFIG_RESOURCE_TYPES.TOPIC,
          name: 'orders',
          configs: [{ name: 'retention.ms', value: null, operation: INCREMENTAL_ALTER_CONFIGS_OPERATIONS.DELETE }],
        },
      ],
      validateOnly: false,
    });
  });

  it('forwards validateOnly and returns broker results', async () => {
    const incrementalAlterConfigs = vi.fn().mockResolvedValue({ resources: [{ resourceName: 'orders' }] });
    const cluster = fakeAdminCluster({ incrementalAlterConfigs });
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    const result = await api.incrementalAlterConfigs({
      resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'orders', configs: validConfigs }],
      validateOnly: true,
    });

    expect(incrementalAlterConfigs).toHaveBeenCalledWith({
      resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'orders', configs: validConfigs }],
      validateOnly: true,
    });
    expect(result).toEqual({ resources: [{ resourceName: 'orders' }] });
  });

  it('retries and rethrows on NOT_CONTROLLER', async () => {
    const error = Object.assign(new Error('not controller'), { type: 'NOT_CONTROLLER' });
    const cluster = fakeAdminCluster({ incrementalAlterConfigs: vi.fn().mockRejectedValue(error) });
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
      retry: { retries: 0 },
    });

    await expect(
      api.incrementalAlterConfigs({
        resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'orders', configs: validConfigs }],
      }),
    ).rejects.toThrow();
  });

  it('bails immediately on a non-retriable broker error', async () => {
    const incrementalAlterConfigs = vi.fn().mockRejectedValue(new Error('boom'));
    const cluster = fakeAdminCluster({ incrementalAlterConfigs });
    const api = createConfigsApi({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(
      api.incrementalAlterConfigs({
        resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'orders', configs: validConfigs }],
      }),
    ).rejects.toThrow('boom');
    expect(incrementalAlterConfigs).toHaveBeenCalledTimes(1);
  });
});
