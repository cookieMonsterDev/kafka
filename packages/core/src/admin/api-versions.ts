import { KafkaNonRetriableError } from '../errors';
import { nodeTarget } from '../cluster/run';
import { apiKeyName } from '../protocol/requests/api-keys';
import { highestCommonVersion, type BrokerVersions } from '../protocol/requests/index';
import { loadImplementedApis, type ImplementedApi } from '../protocol/requests/request-families';
import { retrier as createRetrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown } from './helpers';
import type { ApiVersionDescription, BrokerApiVersionsDescription, DescribeApiVersionsOptions } from './types';

export interface ApiVersionsApi {
  describeApiVersions: (options?: DescribeApiVersionsOptions) => Promise<{ brokers: BrokerApiVersionsDescription[] }>;
}

function validateBrokerIds(brokerIds: unknown): number[] | undefined {
  if (brokerIds === undefined) return undefined;
  if (!Array.isArray(brokerIds) || brokerIds.some((id) => !Number.isInteger(id) || (id as number) < 0)) {
    throw new KafkaNonRetriableError(`Invalid brokerIds ${formatUnknown(brokerIds)}`);
  }
  return brokerIds.length === 0 ? undefined : (brokerIds as number[]);
}

/** Every API in `advertised` or `implemented`, in api key order. */
function describeApis(advertised: BrokerVersions, implemented: readonly ImplementedApi[]): ApiVersionDescription[] {
  const implementedByKey = new Map(implemented.map((api) => [api.apiKey, api]));
  const apiKeys = new Set([...Object.keys(advertised).map(Number), ...implementedByKey.keys()]);

  return [...apiKeys]
    .sort((a, b) => a - b)
    .map((apiKey) => {
      const brokerRange = advertised[apiKey];
      const family = implementedByKey.get(apiKey);
      const apiName = apiKeyName(apiKey);
      const version = brokerRange && family ? highestCommonVersion(family.versions, brokerRange) : null;
      return {
        apiKey,
        ...(apiName === undefined ? {} : { apiName }),
        ...(brokerRange === undefined
          ? {}
          : { brokerVersions: { minVersion: brokerRange.minVersion ?? 0, maxVersion: brokerRange.maxVersion } }),
        isImplemented: family !== undefined,
        ...(version === null ? {} : { version }),
      };
    });
}

export function createApiVersionsApi({ cluster, retry }: AdminContext): ApiVersionsApi {
  const retrier = createRetrier(retry);

  const describeApiVersions = async (
    options: DescribeApiVersionsOptions = {},
  ): Promise<{ brokers: BrokerApiVersionsDescription[] }> => {
    const requestedIds = validateBrokerIds(options.brokerIds);
    const implemented = await loadImplementedApis();

    // Read after each attempt's metadata refresh, so the default covers the brokers known then.
    let nodeIds: number[] = [];
    return cluster.run(
      () => {
        const knownIds = Object.keys(cluster.brokerPool.brokers).map(Number);
        const unknownId = requestedIds?.find((id) => !knownIds.includes(id));
        if (unknownId !== undefined) {
          throw new KafkaNonRetriableError(`Broker ${String(unknownId)} is not in the cluster metadata`);
        }
        nodeIds = requestedIds ?? knownIds;
        return nodeIds.map((nodeId) => nodeTarget(nodeId));
      },
      async (brokers) => {
        if (brokers.length === 0) {
          throw new KafkaNonRetriableError('No brokers available to describe API versions');
        }

        return {
          brokers: await Promise.all(
            brokers.map(async (broker, index) => {
              // A fresh request, since a broker may reuse the versions the bootstrap broker advertised.
              const advertised = await broker.apiVersions();
              const { host, port, rack } = broker.connectionPool;
              return {
                nodeId: Number(nodeIds[index]),
                host,
                port,
                rack,
                apis: describeApis(advertised, implemented),
              };
            }),
          ),
        };
      },
      { retrier, refreshMetadata: true, description: 'Could not describe API versions' },
    );
  };

  return { describeApiVersions };
}
