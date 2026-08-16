import { KafkaJSDeleteGroupsError, KafkaJSNonRetriableError } from '../errors.js';
import type { DescribeGroupsResponseV2Body } from '../protocol/requests/describe-groups/v2/response.js';
import type { DeleteGroupsResult } from '../protocol/requests/delete-groups/v0/response.js';
import type { ListGroupsResponseV2Body } from '../protocol/requests/list-groups/v2/response.js';
import { retrier } from '../retry/index.js';
import type { AdminContext } from './helpers.js';
import { protocolType, formatUnknown } from './helpers.js';

export interface GroupsApi {
  listGroups: () => Promise<{ groups: ListGroupsResponseV2Body['groups'] }>;
  describeGroups: (groupIds: string[]) => Promise<{ groups: DescribeGroupsResponseV2Body['groups'] }>;
  deleteGroups: (groupIds: string[]) => Promise<DeleteGroupsResult[]>;
}

export function createGroupsApi({ cluster, logger, retry }: AdminContext): GroupsApi {
  const listGroups = async (): Promise<{ groups: ListGroupsResponseV2Body['groups'] }> => {
    await cluster.refreshMetadata();
    const groups: ListGroupsResponseV2Body['groups'] = [];

    for (const nodeId of Object.keys(cluster.brokerPool.brokers)) {
      const broker = await cluster.findBroker({ nodeId });
      const response = await broker.listGroups();
      groups.push(...response.groups);
    }

    return { groups };
  };

  const describeGroups = async (groupIds: string[]): Promise<{ groups: DescribeGroupsResponseV2Body['groups'] }> => {
    const coordinatorsForGroup = await Promise.all(
      groupIds.map(async (groupId) => {
        const coordinator = await cluster.findGroupCoordinator({ groupId });
        return { coordinator, groupId };
      }),
    );

    const groupsByCoordinator = new Map<
      string,
      { coordinator: (typeof coordinatorsForGroup)[number]['coordinator']; groupIds: string[] }
    >();
    for (const { coordinator, groupId } of coordinatorsForGroup) {
      const key = String(coordinator.nodeId);
      const existing = groupsByCoordinator.get(key);
      if (existing) {
        existing.groupIds.push(groupId);
      } else {
        groupsByCoordinator.set(key, { coordinator, groupIds: [groupId] });
      }
    }

    const responses = await Promise.all(
      [...groupsByCoordinator.values()].map(async ({ coordinator, groupIds: ids }) => {
        const { groups } = await retrier(retry)(() => coordinator.describeGroups({ groupIds: ids }));
        return groups;
      }),
    );

    return { groups: responses.flat() };
  };

  const deleteGroups = async (groupIds: string[]): Promise<DeleteGroupsResult[]> => {
    if (!groupIds || !Array.isArray(groupIds)) {
      throw new KafkaJSNonRetriableError(`Invalid groupIds array ${formatUnknown(groupIds)}`);
    }

    const invalidGroupId = groupIds.some((groupId) => typeof groupId !== 'string');
    if (invalidGroupId) {
      throw new KafkaJSNonRetriableError(`Invalid groupId name: ${JSON.stringify(invalidGroupId)}`);
    }

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
          throw new KafkaJSDeleteGroupsError('Error in DeleteGroups', errors);
        }

        return responses.flatMap(({ results }) => results);
      } catch (error) {
        const type = protocolType(error);
        if (type === 'NOT_CONTROLLER' || type === 'COORDINATOR_NOT_AVAILABLE') {
          logger.warn('Could not delete groups', {
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
  };

  return { listGroups, describeGroups, deleteGroups };
}
