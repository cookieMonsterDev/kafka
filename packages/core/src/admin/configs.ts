import { KafkaNonRetriableError } from '../errors';
import { CONFIG_RESOURCE_TYPES } from '../protocol/enums/config-resource-types';
import { INCREMENTAL_ALTER_CONFIGS_OPERATIONS } from '../protocol/enums/incremental-alter-configs-operations';
import { staleMetadata } from '../protocol/error-codes';
import type { AlterConfigsResponseV1Body } from '../protocol/requests/alter-configs/v1/response';
import type { DescribeConfigsResponseV2Body } from '../protocol/requests/describe-configs/v2/response';
import type { IncrementalAlterConfigsResponseV1Body } from '../protocol/requests/incremental-alter-configs/v1/response';
import { retrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { groupResourcesByBroker, protocolType, formatUnknown } from './helpers';
import type { IncrementalResourceConfig, ResourceConfig, ResourceConfigQuery } from './types';

export interface ConfigsApi {
  describeConfigs: (options: {
    resources: ResourceConfigQuery[];
    includeSynonyms?: boolean;
    includeDocumentation?: boolean;
  }) => Promise<{ resources: DescribeConfigsResponseV2Body['resources'] }>;
  alterConfigs: (options: {
    resources: ResourceConfig[];
    validateOnly?: boolean;
  }) => Promise<{ resources: AlterConfigsResponseV1Body['resources'] }>;
  incrementalAlterConfigs: (options: {
    resources: IncrementalResourceConfig[];
    validateOnly?: boolean;
  }) => Promise<{ resources: IncrementalAlterConfigsResponseV1Body['resources'] }>;
  listConfigResources: (options?: {
    resourceTypes?: number[];
  }) => Promise<{ resources: Array<{ resourceName: string; resourceType: number }> }>;
}

const VALID_RESOURCE_TYPES: readonly number[] = Object.values(CONFIG_RESOURCE_TYPES);
const VALID_CONFIG_OPERATIONS: readonly number[] = Object.values(INCREMENTAL_ALTER_CONFIGS_OPERATIONS);

export function createConfigsApi({ cluster, logger, retry }: AdminContext): ConfigsApi {
  const describeConfigs = async ({
    resources,
    includeSynonyms,
    includeDocumentation,
  }: {
    resources: ResourceConfigQuery[];
    includeSynonyms?: boolean;
    includeDocumentation?: boolean;
  }): Promise<{ resources: DescribeConfigsResponseV2Body['resources'] }> => {
    if (!resources || !Array.isArray(resources)) {
      throw new KafkaNonRetriableError(`Invalid resources array ${formatUnknown(resources)}`);
    }

    if (resources.length === 0) {
      throw new KafkaNonRetriableError('Resources array cannot be empty');
    }

    const invalidType = resources.find((resource) => !VALID_RESOURCE_TYPES.includes(resource.type));
    if (invalidType) {
      throw new KafkaNonRetriableError(`Invalid resource type ${invalidType.type}: ${JSON.stringify(invalidType)}`);
    }

    const invalidName = resources.find((resource) => !resource.name || typeof resource.name !== 'string');
    if (invalidName) {
      throw new KafkaNonRetriableError(`Invalid resource name ${invalidName.name}: ${JSON.stringify(invalidName)}`);
    }

    const invalidConfigs = resources.find(
      (resource) => !Array.isArray(resource.configNames) && resource.configNames != null,
    );
    if (invalidConfigs) {
      const { configNames } = invalidConfigs;
      throw new KafkaNonRetriableError(
        `Invalid resource configNames ${formatUnknown(configNames)}: ${JSON.stringify(invalidConfigs)}`,
      );
    }

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const controller = await cluster.findControllerBroker();
        const resourcesByBroker = await groupResourcesByBroker({ resources, defaultBroker: controller, cluster });

        const brokers = [...resourcesByBroker.keys()];
        const responses = await Promise.all(
          brokers.map(async (broker) => {
            const targetBroker = broker || controller;
            return targetBroker.describeConfigs({
              resources: resourcesByBroker.get(targetBroker) ?? [],
              includeSynonyms,
              includeDocumentation,
            });
          }),
        );

        return { resources: responses.flatMap((response) => response.resources) };
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER' || staleMetadata({ type: protocolType(error) })) {
          logger.warn('Could not describe configs', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          throw error;
        }

        bail(error as Error);
        return { resources: [] };
      }
    });
  };

  const alterConfigs = async ({
    resources,
    validateOnly,
  }: {
    resources: ResourceConfig[];
    validateOnly?: boolean;
  }): Promise<{ resources: AlterConfigsResponseV1Body['resources'] }> => {
    if (!resources || !Array.isArray(resources)) {
      throw new KafkaNonRetriableError(`Invalid resources array ${formatUnknown(resources)}`);
    }

    if (resources.length === 0) {
      throw new KafkaNonRetriableError('Resources array cannot be empty');
    }

    const invalidType = resources.find((resource) => !VALID_RESOURCE_TYPES.includes(resource.type));
    if (invalidType) {
      throw new KafkaNonRetriableError(`Invalid resource type ${invalidType.type}: ${JSON.stringify(invalidType)}`);
    }

    const invalidName = resources.find((resource) => !resource.name || typeof resource.name !== 'string');
    if (invalidName) {
      throw new KafkaNonRetriableError(`Invalid resource name ${invalidName.name}: ${JSON.stringify(invalidName)}`);
    }

    const invalidConfigs = resources.find((resource) => !Array.isArray(resource.configEntries));
    if (invalidConfigs) {
      const { configEntries } = invalidConfigs;
      throw new KafkaNonRetriableError(
        `Invalid resource configEntries ${formatUnknown(configEntries)}: ${JSON.stringify(invalidConfigs)}`,
      );
    }

    const invalidConfigValue = resources.find((resource) =>
      resource.configEntries.some((entry) => typeof entry.name !== 'string' || typeof entry.value !== 'string'),
    );
    if (invalidConfigValue) {
      throw new KafkaNonRetriableError(`Invalid resource config value: ${JSON.stringify(invalidConfigValue)}`);
    }

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const controller = await cluster.findControllerBroker();
        const resourcesByBroker = await groupResourcesByBroker({ resources, defaultBroker: controller, cluster });

        const brokers = [...resourcesByBroker.keys()];
        const responses = await Promise.all(
          brokers.map(async (broker) => {
            const targetBroker = broker || controller;
            return targetBroker.alterConfigs({
              resources: resourcesByBroker.get(targetBroker) ?? [],
              validateOnly: !!validateOnly,
            });
          }),
        );

        return { resources: responses.flatMap((response) => response.resources) };
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER' || staleMetadata({ type: protocolType(error) })) {
          logger.warn('Could not alter configs', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          throw error;
        }

        bail(error as Error);
        return { resources: [] };
      }
    });
  };

  const incrementalAlterConfigs = async ({
    resources,
    validateOnly,
  }: {
    resources: IncrementalResourceConfig[];
    validateOnly?: boolean;
  }): Promise<{ resources: IncrementalAlterConfigsResponseV1Body['resources'] }> => {
    if (!resources || !Array.isArray(resources)) {
      throw new KafkaNonRetriableError(`Invalid resources array ${formatUnknown(resources)}`);
    }

    if (resources.length === 0) {
      throw new KafkaNonRetriableError('Resources array cannot be empty');
    }

    const invalidType = resources.find((resource) => !VALID_RESOURCE_TYPES.includes(resource.type));
    if (invalidType) {
      throw new KafkaNonRetriableError(`Invalid resource type ${invalidType.type}: ${JSON.stringify(invalidType)}`);
    }

    const invalidName = resources.find((resource) => !resource.name || typeof resource.name !== 'string');
    if (invalidName) {
      throw new KafkaNonRetriableError(`Invalid resource name ${invalidName.name}: ${JSON.stringify(invalidName)}`);
    }

    const invalidConfigs = resources.find((resource) => !Array.isArray(resource.configs));
    if (invalidConfigs) {
      const { configs } = invalidConfigs;
      throw new KafkaNonRetriableError(
        `Invalid resource configs ${formatUnknown(configs)}: ${JSON.stringify(invalidConfigs)}`,
      );
    }

    const invalidConfigValue = resources.find((resource) =>
      resource.configs.some(
        (entry) =>
          typeof entry.name !== 'string' ||
          (entry.value !== null && typeof entry.value !== 'string') ||
          !VALID_CONFIG_OPERATIONS.includes(entry.operation),
      ),
    );
    if (invalidConfigValue) {
      throw new KafkaNonRetriableError(`Invalid resource config value: ${JSON.stringify(invalidConfigValue)}`);
    }

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const controller = await cluster.findControllerBroker();
        const resourcesByBroker = await groupResourcesByBroker({ resources, defaultBroker: controller, cluster });

        const brokers = [...resourcesByBroker.keys()];
        const responses = await Promise.all(
          brokers.map(async (broker) => {
            const targetBroker = broker || controller;
            return targetBroker.incrementalAlterConfigs({
              resources: resourcesByBroker.get(targetBroker) ?? [],
              validateOnly: !!validateOnly,
            });
          }),
        );

        return { resources: responses.flatMap((response) => response.resources) };
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER' || staleMetadata({ type: protocolType(error) })) {
          logger.warn('Could not incrementally alter configs', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          throw error;
        }

        bail(error as Error);
        return { resources: [] };
      }
    });
  };

  const listConfigResources = async ({
    resourceTypes,
  }: {
    resourceTypes?: number[];
  } = {}): Promise<{ resources: Array<{ resourceName: string; resourceType: number }> }> => {
    if (resourceTypes != null) {
      if (!Array.isArray(resourceTypes)) {
        throw new KafkaNonRetriableError(`Invalid resourceTypes array ${formatUnknown(resourceTypes)}`);
      }
      const invalidType = resourceTypes.find((type) => !VALID_RESOURCE_TYPES.includes(type));
      if (invalidType != null) {
        throw new KafkaNonRetriableError(`Invalid resource type ${invalidType}`);
      }
    }

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        const { configResources } = await broker.listConfigResources({ resourceTypes });
        return { resources: configResources };
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not list config resources', {
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

  return { describeConfigs, alterConfigs, incrementalAlterConfigs, listConfigResources };
}
