import type { Broker } from '../broker/index';
import { COORDINATOR_LOADING_ERRORS, isStaleMetadata } from '../cluster/error-classifier';
import type { Cluster } from '../cluster/index';
import { coordinatorTarget } from '../cluster/run';
import { KafkaDeleteGroupsError, KafkaMetadataNotLoaded, KafkaNonRetriableError } from '../errors';
import type { Logger } from '../loggers/index';
import { CONFIG_RESOURCE_TYPES } from '../protocol/enums/config-resource-types';
import type { DeleteGroupsResult } from '../protocol/requests/delete-groups/v0/response';
import type { ClusterMetadata } from '../protocol/requests/metadata/shared';
import { retrier, type RetryOptions } from '../retry/index';
import { groupBy } from '../utils/group-by';
import { waitFor, type WaitForOptions } from '../utils/wait';
import { DeleteGroups } from '../protocol/requests/lazy-families';

export interface AdminContext {
  cluster: Cluster;
  logger: Logger;
  rootLogger: Logger;
  retry?: RetryOptions;
}

export function formatUnknown(value: unknown): string {
  return String(value);
}

export function assertValidGroupIds(groupIds: unknown): asserts groupIds is string[] {
  if (!Array.isArray(groupIds)) {
    throw new KafkaNonRetriableError(`Invalid groupIds array ${formatUnknown(groupIds)}`);
  }
  if (groupIds.some((groupId) => typeof groupId !== 'string' || groupId === '')) {
    throw new KafkaNonRetriableError('Group IDs must be non-empty strings');
  }
}

export async function retryOnLeaderNotAvailable<T>(fn: () => Promise<T>, options: WaitForOptions = {}): Promise<T> {
  return waitFor(async () => {
    try {
      return await fn();
    } catch (error) {
      if (!isStaleMetadata(error)) {
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

/** Groups `items` by the broker at the same index, keeping the first-seen order. */
export function groupByBroker<T>(items: readonly T[], brokers: readonly Broker[]): Map<Broker, T[]> {
  const itemsByBroker = new Map<Broker, T[]>();
  items.forEach((item, index) => {
    const broker = brokers[index]!;
    const existing = itemsByBroker.get(broker);
    if (existing) {
      existing.push(item);
    } else {
      itemsByBroker.set(broker, [item]);
    }
  });
  return itemsByBroker;
}

/**
 * Batches a per-coordinator describe request for each group id. Coordinators are resolved on every
 * attempt, so a retry after NOT_COORDINATOR_FOR_GROUP targets the group's current coordinator.
 */
export async function describeGroupsViaCoordinators<T>({
  cluster,
  retry,
  groupIds,
  describe,
}: {
  cluster: Cluster;
  retry?: RetryOptions;
  groupIds: readonly string[];
  describe: (coordinator: Broker, groupIds: string[]) => Promise<{ groups: T[] }>;
}): Promise<T[]> {
  const responses = await cluster.run(
    () => groupIds.map((groupId) => coordinatorTarget(groupId)),
    (coordinators) =>
      Promise.all([...groupByBroker(groupIds, coordinators)].map(([coordinator, ids]) => describe(coordinator, ids))),
    { retrier: retrier(retry), retryRetriable: true },
  );

  return responses.flatMap(({ groups }) => groups);
}

/**
 * Shared coordinator-resolution retry loop backing `deleteGroups`/`deleteShareGroups`: resolves
 * each remaining group id's coordinator, batches `DeleteGroups` per coordinator, and retries only
 * the groups that came back with an error, while every such error is a moved or loading coordinator.
 */
export function deleteGroupsViaCoordinators({
  cluster,
  retry,
  groupIds,
  errorLabel,
  logMessage,
}: {
  cluster: Cluster;
  retry?: RetryOptions;
  groupIds: readonly string[];
  errorLabel: string;
  logMessage: string;
}): Promise<DeleteGroupsResult[]> {
  if (groupIds.length === 0) return Promise.resolve([]);

  let remaining = groupIds.slice();
  const deleted: DeleteGroupsResult[] = [];

  return cluster.run(
    () => remaining.map((groupId) => coordinatorTarget(groupId)),
    async (coordinators) => {
      const responses = await Promise.all(
        [...groupByBroker(remaining, coordinators)].map(([coordinator, ids]) =>
          coordinator.send(DeleteGroups, { groupIds: ids }),
        ),
      );

      const results = responses.flatMap((response) => response.results);
      const errors = results
        .filter(({ errorCode }) => errorCode !== 0)
        .map(({ groupId, errorCode, error }) => ({ groupId, errorCode, error }));

      deleted.push(...results.filter(({ errorCode }) => errorCode === 0));
      remaining = errors.map(({ groupId }) => groupId);

      if (errors.length > 0) {
        throw new KafkaDeleteGroupsError(errorLabel, errors);
      }

      return deleted;
    },
    { retrier: retrier(retry), refreshMetadata: true, retryOn: COORDINATOR_LOADING_ERRORS, description: logMessage },
  );
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
