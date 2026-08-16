import { KafkaJSNonRetriableError } from '../errors.js';
import { CONFIG_RESOURCE_TYPES } from '../protocol/enums/config-resource-types.js';
import type { AlterConfigsResponseV1Body } from '../protocol/requests/alter-configs/v1/response.js';
import type { DescribeConfigsResponseV2Body } from '../protocol/requests/describe-configs/v2/response.js';
import { retrier } from '../retry/index.js';
import type { AdminContext } from './helpers.js';
import { groupResourcesByBroker, protocolType, formatUnknown } from './helpers.js';
import type { ResourceConfig, ResourceConfigQuery } from './types.js';

export interface ConfigsApi {
  describeConfigs: (options: {
    resources: ResourceConfigQuery[];
    includeSynonyms?: boolean;
  }) => Promise<{ resources: DescribeConfigsResponseV2Body['resources'] }>;
  alterConfigs: (options: {
    resources: ResourceConfig[];
    validateOnly?: boolean;
  }) => Promise<{ resources: AlterConfigsResponseV1Body['resources'] }>;
}

const VALID_RESOURCE_TYPES = Object.values(CONFIG_RESOURCE_TYPES);

export function createConfigsApi({ cluster, logger, retry }: AdminContext): ConfigsApi {
  const describeConfigs = async ({
    resources,
    includeSynonyms,
  }: {
    resources: ResourceConfigQuery[];
    includeSynonyms?: boolean;
  }): Promise<{ resources: DescribeConfigsResponseV2Body['resources'] }> => {
    if (!resources || !Array.isArray(resources)) {
      throw new KafkaJSNonRetriableError(`Invalid resources array ${formatUnknown(resources)}`);
    }

    if (resources.length === 0) {
      throw new KafkaJSNonRetriableError('Resources array cannot be empty');
    }

    const invalidType = resources.find((resource) => !VALID_RESOURCE_TYPES.includes(resource.type));
    if (invalidType) {
      throw new KafkaJSNonRetriableError(`Invalid resource type ${invalidType.type}: ${JSON.stringify(invalidType)}`);
    }

    const invalidName = resources.find((resource) => !resource.name || typeof resource.name !== 'string');
    if (invalidName) {
      throw new KafkaJSNonRetriableError(`Invalid resource name ${invalidName.name}: ${JSON.stringify(invalidName)}`);
    }

    const invalidConfigs = resources.find(
      (resource) => !Array.isArray(resource.configNames) && resource.configNames != null,
    );
    if (invalidConfigs) {
      const { configNames } = invalidConfigs;
      throw new KafkaJSNonRetriableError(
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
            });
          }),
        );

        return { resources: responses.flatMap((response) => response.resources) };
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
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
      throw new KafkaJSNonRetriableError(`Invalid resources array ${formatUnknown(resources)}`);
    }

    if (resources.length === 0) {
      throw new KafkaJSNonRetriableError('Resources array cannot be empty');
    }

    const invalidType = resources.find((resource) => !VALID_RESOURCE_TYPES.includes(resource.type));
    if (invalidType) {
      throw new KafkaJSNonRetriableError(`Invalid resource type ${invalidType.type}: ${JSON.stringify(invalidType)}`);
    }

    const invalidName = resources.find((resource) => !resource.name || typeof resource.name !== 'string');
    if (invalidName) {
      throw new KafkaJSNonRetriableError(`Invalid resource name ${invalidName.name}: ${JSON.stringify(invalidName)}`);
    }

    const invalidConfigs = resources.find((resource) => !Array.isArray(resource.configEntries));
    if (invalidConfigs) {
      const { configEntries } = invalidConfigs;
      throw new KafkaJSNonRetriableError(
        `Invalid resource configEntries ${formatUnknown(configEntries)}: ${JSON.stringify(invalidConfigs)}`,
      );
    }

    const invalidConfigValue = resources.find((resource) =>
      resource.configEntries.some((entry) => typeof entry.name !== 'string' || typeof entry.value !== 'string'),
    );
    if (invalidConfigValue) {
      throw new KafkaJSNonRetriableError(`Invalid resource config value: ${JSON.stringify(invalidConfigValue)}`);
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
        if (protocolType(error) === 'NOT_CONTROLLER') {
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

  return { describeConfigs, alterConfigs };
}
