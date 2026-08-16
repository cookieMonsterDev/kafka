import type { Broker } from '../broker/index.js';
import type { Cluster } from '../cluster/index.js';
import { KafkaJSMetadataNotLoaded } from '../errors.js';
import type { Logger } from '../loggers/index.js';
import { CONFIG_RESOURCE_TYPES } from '../protocol/enums/config-resource-types.js';
import { staleMetadata } from '../protocol/error-codes.js';
import type { MetadataResponseV6Body } from '../protocol/requests/metadata/v6/response.js';
import type { RetryOptions } from '../retry/index.js';
import { groupBy } from '../utils/group-by.js';
import { waitFor, type WaitForOptions } from '../utils/wait.js';

export interface AdminContext {
  cluster: Cluster;
  logger: Logger;
  rootLogger: Logger;
  retry?: RetryOptions;
}

export function formatUnknown(value: unknown): string {
  return String(value);
}

export function protocolType(error: unknown): string | undefined {
  if (error != null && typeof error === 'object' && 'type' in error) {
    const type = (error as { type?: unknown }).type;
    return typeof type === 'string' ? type : undefined;
  }
  return undefined;
}

export async function retryOnLeaderNotAvailable<T>(fn: () => Promise<T>, options: WaitForOptions = {}): Promise<T> {
  return waitFor(async () => {
    try {
      return await fn();
    } catch (error) {
      if (!staleMetadata({ type: protocolType(error) })) {
        throw error;
      }
      return false;
    }
  }, options);
}

export async function findTopicPartitions(cluster: Cluster, topic: string): Promise<number[]> {
  await cluster.addTargetTopic(topic);
  await cluster.refreshMetadataIfNecessary();

  return cluster
    .findTopicPartitionMetadata(topic)
    .map(({ partitionId }) => partitionId)
    .sort((a, b) => a - b);
}

export async function requireMetadata(
  cluster: Cluster,
  options: { topics?: readonly string[] } = {},
): Promise<MetadataResponseV6Body> {
  const metadata = await cluster.metadata(options);
  if (!metadata) {
    throw new KafkaJSMetadataNotLoaded('Topic metadata not loaded');
  }
  return metadata;
}

export function isConsumerGroupIdle(state: string): boolean {
  return state === 'Empty' || state === 'Dead';
}

export function isBrokerConfig(type: number): boolean {
  return type === CONFIG_RESOURCE_TYPES.BROKER || type === CONFIG_RESOURCE_TYPES.BROKER_LOGGER;
}

export async function groupResourcesByBroker<T extends { type: number; name: string }>({
  cluster,
  resources,
  defaultBroker,
}: {
  cluster: Cluster;
  resources: readonly T[];
  defaultBroker: Broker;
}): Promise<Map<Broker, T[]>> {
  return groupBy(resources, async ({ type, name: nodeId }) =>
    isBrokerConfig(type) ? cluster.findBroker({ nodeId: String(nodeId) }) : defaultBroker,
  );
}
