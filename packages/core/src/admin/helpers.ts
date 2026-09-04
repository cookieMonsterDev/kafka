import type { Broker } from '../broker/index';
import type { Cluster } from '../cluster/index';
import { KafkaDeleteGroupsError, KafkaMetadataNotLoaded } from '../errors';
import type { Logger } from '../loggers/index';
import { CONFIG_RESOURCE_TYPES } from '../protocol/enums/config-resource-types';
import { staleMetadata } from '../protocol/error-codes';
import type { DeleteGroupsResult } from '../protocol/requests/delete-groups/v0/response';
import type { ClusterMetadata } from '../protocol/requests/metadata/shared';
import { retrier, type RetryOptions } from '../retry/index';
import { groupBy } from '../utils/group-by';
import { waitFor, type WaitForOptions } from '../utils/wait';

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
): Promise<ClusterMetadata> {
  const metadata = await cluster.metadata(options);
  if (!metadata) {
    throw new KafkaMetadataNotLoaded('Topic metadata not loaded');
  }
  return metadata;
}

export function isConsumerGroupIdle(state: string): boolean {
  return state === 'Empty' || state === 'Dead';
}

export function isBrokerConfig(type: number): boolean {
  return type === CONFIG_RESOURCE_TYPES.BROKER || type === CONFIG_RESOURCE_TYPES.BROKER_LOGGER;
}

/**
 * Shared coordinator-resolution retry loop backing `deleteGroups`/`deleteShareGroups`: resolves
 * each remaining group id's coordinator, batches `DeleteGroups` per coordinator node, and retries
 * only the groups that came back with an error, until the retrier gives up or bails.
 */
export function deleteGroupsViaCoordinators({
  cluster,
  logger,
  retry,
  groupIds,
  errorLabel,
  logMessage,
}: {
  cluster: Cluster;
  logger: Logger;
  retry?: RetryOptions;
  groupIds: readonly string[];
  errorLabel: string;
  logMessage: string;
}): Promise<DeleteGroupsResult[]> {
  let remaining = groupIds.slice();

  return retrier(retry)(async (bail, retryCount, retryTime) => {
    try {
      if (remaining.length === 0) return [];

      await cluster.refreshMetadata();

      const groupsByNode = new Map<string, string[]>();
      const brokerByNode = new Map<string, Awaited<ReturnType<typeof cluster.findGroupCoordinator>>>();

      for (const groupId of remaining) {
        const broker = await cluster.findGroupCoordinator({ groupId });
        const nodeId = String(broker.nodeId);
        const existing = groupsByNode.get(nodeId) ?? [];
        existing.push(groupId);
        groupsByNode.set(nodeId, existing);
        brokerByNode.set(nodeId, broker);
      }

      const responses = await Promise.all(
        [...brokerByNode.entries()].map(async ([nodeId, broker]) =>
          broker.deleteGroups({ groupIds: groupsByNode.get(nodeId) ?? [] }),
        ),
      );

      const errors = responses
        .flatMap(({ results }) => results.map(({ groupId, errorCode, error }) => ({ groupId, errorCode, error })))
        .filter(({ errorCode }) => errorCode !== 0);

      remaining = errors.map(({ groupId }) => groupId);

      if (errors.length > 0) {
        throw new KafkaDeleteGroupsError(errorLabel, errors);
      }

      return responses.flatMap(({ results }) => results);
    } catch (error) {
      const type = protocolType(error);
      if (type === 'NOT_CONTROLLER' || type === 'COORDINATOR_NOT_AVAILABLE') {
        logger.warn(logMessage, {
          error: error instanceof Error ? error.message : String(error),
          retryCount,
          retryTime,
        });
        throw error;
      }

      bail(error as Error);
      return [];
    }
  });
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
